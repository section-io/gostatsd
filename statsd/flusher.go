package statsd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

const (
	internalMetric           = "statsd."
	badLinesSeenSuffix       = ".bad_lines_seen"
	metricsReceivedSuffix    = ".metrics_received"
	packetsReceivedSuffix    = ".packets_received"
	numStatsSuffix           = ".numStats"
	aggregatorNumStatsSuffix = ".aggregator_num_stats"
	processingTimeSuffix     = ".processing_time"
)

// FlusherStats holds statistics about a Flusher.
type FlusherStats struct {
	LastFlush      time.Time // Last time the metrics where aggregated
	LastFlushError time.Time // Time of the last flush error
}

// Flusher periodically flushes metrics from all Aggregators to Senders.
type Flusher interface {
	Run(context.Context) error
	GetStats() FlusherStats
}

type flusher struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastFlush      int64 // Last time the metrics where aggregated. Unix timestamp in nsec.
	lastFlushError int64 // Time of the last flush error. Unix timestamp in nsec.

	flushInterval time.Duration // How often to flush metrics to the sender
	dispatcher    Dispatcher
	receiver      Receiver
	handler       Handler
	backends      []backendTypes.Backend
	selfIP        types.IP
	hostname      string

	// Sent statistics for Receiver. Keep sent values to calculate diff.
	sentBadLines        uint64
	sentPacketsReceived uint64
	sentMetricsReceived uint64

	// Metric names
	badLinesSeen       string
	metricsReceived    string
	packetsReceived    string
	numStats           string
	aggregatorNumStats string
	processingTime     string
}

// NewFlusher creates a new Flusher with provided configuration.
func NewFlusher(flushInterval time.Duration, dispatcher Dispatcher, receiver Receiver, handler Handler, backends []backendTypes.Backend, selfIP types.IP, hostname string) Flusher {
	return &flusher{
		flushInterval: flushInterval,
		dispatcher:    dispatcher,
		receiver:      receiver,
		handler:       handler,
		backends:      backends,
		selfIP:        selfIP,
		hostname:      hostname,

		badLinesSeen:       internalMetric + hostname + badLinesSeenSuffix,
		metricsReceived:    internalMetric + hostname + metricsReceivedSuffix,
		packetsReceived:    internalMetric + hostname + packetsReceivedSuffix,
		numStats:           internalMetric + hostname + numStatsSuffix,
		aggregatorNumStats: internalMetric + hostname + aggregatorNumStatsSuffix,
		processingTime:     internalMetric + hostname + processingTimeSuffix,
	}
}

// Run runs the Flusher.
func (f *flusher) Run(ctx context.Context) error {
	flushTicker := time.NewTicker(f.flushInterval)
	defer flushTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flushTicker.C: // Time to flush to the backends
			dispatcherStats := f.flushData(ctx)
			f.dispatchInternalStats(ctx, dispatcherStats)
		}
	}
}

// GetStats returns Flusher statistics.
func (f *flusher) GetStats() FlusherStats {
	return FlusherStats{
		time.Unix(0, atomic.LoadInt64(&f.lastFlush)),
		time.Unix(0, atomic.LoadInt64(&f.lastFlushError)),
	}
}

func (f *flusher) flushData(ctx context.Context) map[uint16]types.MetricStats {
	var lock sync.Mutex
	dispatcherStats := make(map[uint16]types.MetricStats)
	var sendWg sync.WaitGroup
	processWg := f.dispatcher.Process(ctx, func(workerId uint16, aggr Aggregator) {
		aggr.Flush(f.flushInterval)
		aggr.Process(func(m *types.MetricMap) {
			f.sendMetricsAsync(ctx, &sendWg, m)
			lock.Lock()
			defer lock.Unlock()
			dispatcherStats[workerId] = m.MetricStats
		})
		aggr.Reset()
	})
	processWg.Wait() // Wait for all workers to execute function
	sendWg.Wait()    // Wait for all backends to finish sending

	return dispatcherStats
}

func (f *flusher) sendMetricsAsync(ctx context.Context, wg *sync.WaitGroup, m *types.MetricMap) {
	wg.Add(len(f.backends))
	for _, backend := range f.backends {
		log.Debugf("Sending %d metrics to backend %s", m.NumStats, backend.BackendName())
		backend.SendMetricsAsync(ctx, m, func(errs []error) {
			defer wg.Done()
			log.Debugf("Send of %d metrics to backend %s completed with %d errors", m.NumStats, backend.BackendName(), len(errs))
			f.handleSendResult(errs)
		})
	}
}

func (f *flusher) handleSendResult(flushResults []error) {
	timestampPointer := &f.lastFlush
	for _, err := range flushResults {
		if err != nil {
			timestampPointer = &f.lastFlushError
			log.Errorf("Sending metrics to backend failed: %v", err)
		}
	}
	atomic.StoreInt64(timestampPointer, time.Now().UnixNano())
}

func (f *flusher) dispatchInternalStats(ctx context.Context, dispatcherStats map[uint16]types.MetricStats) {
	receiverStats := f.receiver.GetStats()
	packetsReceivedValue := receiverStats.PacketsReceived - f.sentPacketsReceived
	metrics := make([]types.Metric, 0, 4+2*len(dispatcherStats))
	metrics = append(metrics,
		types.Metric{
			Name:  f.badLinesSeen,
			Value: float64(receiverStats.BadLines - f.sentBadLines),
			Type:  types.COUNTER,
		},
		types.Metric{
			Name:  f.metricsReceived,
			Value: float64(receiverStats.MetricsReceived - f.sentMetricsReceived),
			Type:  types.COUNTER,
		},
		types.Metric{
			Name:  f.packetsReceived,
			Value: float64(packetsReceivedValue),
			Type:  types.COUNTER,
		})
	var totalStats uint32
	for workerID, stat := range dispatcherStats {
		totalStats += stat.NumStats
		tag := fmt.Sprintf("aggregator_id:%d", workerID)
		metrics = append(metrics,
			types.Metric{
				Name:  f.aggregatorNumStats,
				Value: float64(stat.NumStats),
				Tags:  types.Tags{tag},
				Type:  types.COUNTER,
			},
			types.Metric{
				Name:  f.processingTime,
				Value: float64(stat.ProcessingTime) / float64(time.Millisecond),
				Tags:  types.Tags{tag},
				Type:  types.GAUGE,
			})
	}
	metrics = append(metrics, types.Metric{
		Name:  f.numStats,
		Value: float64(totalStats),
		Type:  types.COUNTER,
	})
	log.Debugf("numStats: %d packetsReceived: %d", totalStats, packetsReceivedValue)

	f.sentBadLines = receiverStats.BadLines
	f.sentMetricsReceived = receiverStats.MetricsReceived
	f.sentPacketsReceived = receiverStats.PacketsReceived

	for _, metric := range metrics {
		m := metric // Copy into a new variable
		m.SourceIP = f.selfIP
		m.Hostname = f.hostname
		if err := f.handler.DispatchMetric(ctx, &m); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			log.Warnf("Failed to dispatch internal metric: %v", err)
		}
	}
}

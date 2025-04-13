package bookkeepergo

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	PromNameSpace = "journal"
)

var (
	forceWriteQueueSize             prometheus.Counter
	flushMaxWaitCounter             prometheus.Counter
	flushMaxOutstandingBytesCounter prometheus.Counter
	journalQueueSize                prometheus.Counter
	flushEmptyQueueCounter          prometheus.Counter
	journalWriteBytes               prometheus.Counter
	sdkMetrics                      *prometheus.CounterVec
	journalAddEntryStats            *prometheus.HistogramVec
	journalSyncStats                *prometheus.HistogramVec
	forceWriteGroupingCountStats    *prometheus.HistogramVec
	journalCreationStats            *prometheus.HistogramVec
	journalProcessTimeStats         *prometheus.HistogramVec
	journalFlushStats               *prometheus.HistogramVec
	forceWriteBatchEntriesStats     *prometheus.HistogramVec
	forceWriteBatchBytesStats       *prometheus.HistogramVec
	journalQueueStats               *prometheus.HistogramVec
)

func init() {
	journalQueueSize = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: PromNameSpace,
			Subsystem: "",
			Name:      "JOURNAL_QUEUE_SIZE",
			Help:      "The journal queue size",
		})
	_ = prometheus.Register(journalQueueSize)

	journalWriteBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: PromNameSpace,
			Subsystem: "",
			Name:      "JOURNAL_WRITE_BYTES",
			Help:      "The number of bytes appended to the journal",
		})
	_ = prometheus.Register(journalWriteBytes)

	forceWriteQueueSize = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: PromNameSpace,
			Subsystem: "",
			Name:      "JOURNAL_FORCE_WRITE_QUEUE_SIZE",
			Help:      "The force write queue size",
		})
	_ = prometheus.Register(forceWriteQueueSize)

	flushMaxWaitCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: PromNameSpace,
			Subsystem: "",
			Name:      "JOURNAL_NUM_FLUSH_MAX_WAIT",
			Help:      "The number of journal flushes triggered by MAX_WAIT time",
		})
	_ = prometheus.Register(flushMaxWaitCounter)

	flushMaxOutstandingBytesCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: PromNameSpace,
			Subsystem: "",
			Name:      "JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES",
			Help:      "The number of journal flushes triggered by MAX_OUTSTANDING_BYTES",
		})
	_ = prometheus.Register(flushMaxOutstandingBytesCounter)

	flushEmptyQueueCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: PromNameSpace,
			Subsystem: "",
			Name:      "JOURNAL_NUM_FLUSH_EMPTY_QUEUE",
			Help:      "The number of journal flushes triggered when journal queue becomes empty",
		})
	_ = prometheus.Register(flushEmptyQueueCounter)

	sdkMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: PromNameSpace,
		Subsystem: "metric",
		Name:      "metadata",
		Help:      "tempus sdk metric",
	}, []string{"zone", "appid", "app_name", "version"})
	_ = prometheus.Register(sdkMetrics)

	journalAddEntryStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_ADD_ENTRY",
		Help:      "operation stats of recording addEntry requests in the journal",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(journalAddEntryStats)

	journalSyncStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_SYNC",
		Help:      "operation stats of syncing data to journal disks",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(journalSyncStats)

	forceWriteGroupingCountStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_FORCE_WRITE_GROUPING_TOTAL",
		Help:      "The distribution of number of force write requests grouped in a force write",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(forceWriteGroupingCountStats)

	journalCreationStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_CREATION_LATENCY",
		Help:      "operation stats of creating journal files",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(journalCreationStats)

	journalProcessTimeStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_PROCESS_TIME_LATENCY",
		Help:      "operation stats of processing requests in a journal (from dequeue an item to finish processing it)",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(journalProcessTimeStats)

	journalFlushStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_FLUSH_LATENCY",
		Help:      "operation stats of flushing data from memory to filesystem (but not yet fsyncing to disks)",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(journalFlushStats)

	forceWriteBatchEntriesStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_FORCE_WRITE_BATCH_ENTRIES",
		Help:      "The distribution of number of entries grouped together into a force write request",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(forceWriteBatchEntriesStats)

	forceWriteBatchBytesStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_FORCE_WRITE_BATCH_BYTES",
		Help:      "The distribution of number of bytes grouped together into a force write request",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(forceWriteBatchBytesStats)

	journalQueueStats = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNameSpace,
		Subsystem: "",
		Name:      "JOURNAL_QUEUE_LATENCY",
		Help:      "operation stats of enqueuing requests to a journal",
		Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"success"})
	_ = prometheus.Register(journalQueueStats)
}

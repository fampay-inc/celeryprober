package main

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics contains all the Prometheus metrics for the application
type Metrics struct {
	taskSentTotal      *prometheus.CounterVec
	taskReceivedTotal  *prometheus.CounterVec
	taskStartedTotal   *prometheus.CounterVec
	taskSucceededTotal *prometheus.CounterVec
	taskFailedTotal    *prometheus.CounterVec
	taskDropTotal      *prometheus.CounterVec

	taskProcessingTime   *prometheus.HistogramVec
	taskQueueLatency     *prometheus.HistogramVec
	taskEndToEndDuration *prometheus.HistogramVec

	taskInProgress *prometheus.GaugeVec

	registry *prometheus.Registry
}

var (
	AppMetrics *Metrics
	metricsOnce sync.Once
)

// InitMetrics initializes all metrics with proper naming conventions
func InitMetrics() *Metrics {
	var m *Metrics

	metricsOnce.Do(func() {
		registry := prometheus.NewRegistry()

		// Register the Go collector (collects runtime metrics about the Go process)
		registry.MustRegister(collectors.NewGoCollector())
		// Register process collector (collects metrics about the process)
		registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

		// Create a metrics factory that automatically registers metrics with our registry
		factory := promauto.With(registry)

		// Common labels for all metrics
		commonLabels := prometheus.Labels{}

		// Define custom histogram buckets (in seconds)
		customBuckets := []float64{0.5, 1, 2.5, 5, 10, 15, 30, 60, 120, 300}

		m = &Metrics{
			registry: registry,

			// Task event counters
			taskSentTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "sent_total",
					Help:        "Total number of celery tasks sent",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "queue", "probe_name"},
			),

			taskReceivedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "received_total",
					Help:        "Total number of celery tasks received",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "probe_name"},
			),

			taskStartedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "started_total",
					Help:        "Total number of celery tasks started",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "probe_name"},
			),

			taskSucceededTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "succeeded_total",
					Help:        "Total number of celery tasks succeeded",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "probe_name"},
			),

			taskFailedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "failed_total",
					Help:        "Total number of celery tasks failed",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "probe_name"},
			),

			taskDropTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "drop_total",
					Help:        "Total number of celery tasks dropped",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "last_event", "probe_name"},
			),

			// Timing metrics
			taskProcessingTime: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "processing_seconds",
					Help:        "Time taken to process tasks",
					ConstLabels: commonLabels,
					Buckets:     customBuckets, // Use custom buckets
				},
				[]string{"task_name", "probe_name"},
			),

			taskQueueLatency: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "queue_latency_seconds",
					Help:        "Time spent in queue before processing",
					ConstLabels: commonLabels,
					Buckets:     customBuckets, // Use custom buckets
				},
				[]string{"task_name", "probe_name"},
			),

			taskEndToEndDuration: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "end_to_end_seconds",
					Help:        "Total time from task sent to completion",
					ConstLabels: commonLabels,
					Buckets:     customBuckets, // Use custom buckets
				},
				[]string{"task_name", "status", "probe_name"},
			),

			// System metrics
			taskInProgress: factory.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "in_progress",
					Help:        "Number of tasks currently in progress",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "probe_name"},
			),
		}

		AppMetrics = m
	})

	return AppMetrics
}

func (m *Metrics) RecordTaskSent(taskName, queue, probeName string) {
	m.taskSentTotal.WithLabelValues(taskName, queue, probeName).Inc()
}

func (m *Metrics) RecordTaskReceived(taskName, probeName string) {
	m.taskReceivedTotal.WithLabelValues(taskName, probeName).Inc()
}

func (m *Metrics) RecordTaskStarted(taskName, probeName string) {
	m.taskStartedTotal.WithLabelValues(taskName, probeName).Inc()
	m.taskInProgress.WithLabelValues(taskName, probeName).Inc()
}

func (m *Metrics) RecordTaskSucceeded(taskName string, processingTime float64, probeName string) {
	m.taskSucceededTotal.WithLabelValues(taskName, probeName).Inc()
	m.taskInProgress.WithLabelValues(taskName, probeName).Dec()
	m.taskProcessingTime.WithLabelValues(taskName, probeName).Observe(processingTime)
}

func (m *Metrics) RecordTaskFailed(taskName string, processingTime float64, probeName string) {
	m.taskFailedTotal.WithLabelValues(taskName, probeName).Inc()
	m.taskInProgress.WithLabelValues(taskName, probeName).Dec()
	m.taskProcessingTime.WithLabelValues(taskName, probeName).Observe(processingTime)
}

func (m *Metrics) RecordTaskDrop(taskName, lastEvent, probeName string) {
	m.taskDropTotal.WithLabelValues(taskName, lastEvent, probeName).Inc()
}

func (m *Metrics) RecordTaskQueueLatency(taskName string, latency float64, probeName string) {
	m.taskQueueLatency.WithLabelValues(taskName, probeName).Observe(latency)
}

func (m *Metrics) RecordTaskEndToEnd(taskName, status string, duration float64, probeName string) {
	m.taskEndToEndDuration.WithLabelValues(taskName, status, probeName).Observe(duration)
}

var (
	globalRegistry *prometheus.Registry
)

func init() {
	// Initialize the global registry once during package initialization
	globalRegistry = prometheus.NewRegistry()

	// Register standard collectors
	globalRegistry.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
}

// registerProbeMetrics adds probe-specific metrics to the global registry
// RegisterMetrics registers all metrics with the global registry
// This ensures metrics are registered once at the application level, regardless of probe configuration
func RegisterMetrics() {
	if pm := NewProbeManager(Config); len(pm.Probes) > 0 {
		// Create application-level metrics once
		appMetrics := InitMetrics()
		
		Log.Info().Msg("Registering application-level metrics")
		
		// Register all application metrics with the global registry at once
		// This approach is more maintainable than registering each metric individually
		metricsToRegister := []prometheus.Collector{
			// Task event counters
			appMetrics.taskSentTotal,
			appMetrics.taskReceivedTotal,
			appMetrics.taskStartedTotal,
			appMetrics.taskSucceededTotal,
			appMetrics.taskFailedTotal,
			appMetrics.taskDropTotal,
			
			// Task timing metrics
			appMetrics.taskProcessingTime,
			appMetrics.taskQueueLatency,
			appMetrics.taskEndToEndDuration,
			
			// System metrics
			appMetrics.taskInProgress,
		}
		
		// Register all metrics at once
		for _, metric := range metricsToRegister {
			globalRegistry.MustRegister(metric)
		}
		
		// Register probe info metrics for each enabled probe
		RegisterProbeInfoMetrics(pm.Probes)
		
		Log.Info().Int("probes", len(pm.Probes)).Msg("Registered metrics for all probes")
	}
}

// RegisterProbeInfoMetrics registers probe-specific info metrics
func RegisterProbeInfoMetrics(probes map[string]*Probe) {
	for name, probe := range probes {
		if probe.Config.Enabled && probe.Metrics != nil {
			Log.Info().Str("probe", name).Msg("Adding probe info metric")
			
			// Add probe info metric - other metrics are registered at the application level
			globalRegistry.MustRegister(prometheus.NewGaugeFunc(
				prometheus.GaugeOpts{
					Namespace: "celery_monitor",
					Name:      "probe_info",
					Help:      "Information about the probe",
					ConstLabels: prometheus.Labels{
						"probe":       name,
						"description": probe.Config.Description,
						"redis_url":   probe.Config.CeleryRedisBrokerURL,
					},
				},
				func() float64 { return 1 },
			))
		}
	}
}

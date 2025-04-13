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
func InitMetrics(serviceName string) *Metrics {
	var m *Metrics

	metricsOnce.Do(func() {
		registry := prometheus.NewRegistry()

		// Register the Go collector (collects runtime metrics about the Go process)
		registry.MustRegister(prometheus.NewGoCollector())
		// Register process collector (collects metrics about the process)
		registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

		// Create a metrics factory that automatically registers metrics with our registry
		factory := promauto.With(registry)

		// Common labels for all metrics
		commonLabels := prometheus.Labels{"service": serviceName}

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
				[]string{"task_name", "queue"},
			),

			taskReceivedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "received_total",
					Help:        "Total number of celery tasks received",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),

			taskStartedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "started_total",
					Help:        "Total number of celery tasks started",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),

			taskSucceededTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "succeeded_total",
					Help:        "Total number of celery tasks succeeded",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),

			taskFailedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "failed_total",
					Help:        "Total number of celery tasks failed",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),

			taskDropTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "drop_total",
					Help:        "Total number of celery tasks dropped",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "last_event"},
			),

			// Timing metrics
			taskProcessingTime: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "processing_seconds",
					Help:        "Time taken to process tasks",
					ConstLabels: commonLabels,
					Buckets:     prometheus.DefBuckets,
				},
				[]string{"task_name"},
			),

			taskQueueLatency: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "queue_latency_seconds",
					Help:        "Time spent in queue before processing",
					ConstLabels: commonLabels,
					Buckets:     prometheus.DefBuckets,
				},
				[]string{"task_name"},
			),

			taskEndToEndDuration: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "celery",
					Subsystem:   "tasks",
					Name:        "end_to_end_seconds",
					Help:        "Total time from task sent to completion",
					ConstLabels: commonLabels,
					Buckets:     prometheus.DefBuckets,
				},
				[]string{"task_name", "status"},
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
				[]string{"task_name"},
			),
		}

		AppMetrics = m
	})

	return AppMetrics
}

func (m *Metrics) RecordTaskSent(taskName, queue string) {
	m.taskSentTotal.WithLabelValues(taskName, queue).Inc()
}

func (m *Metrics) RecordTaskReceived(taskName string) {
	m.taskReceivedTotal.WithLabelValues(taskName).Inc()
}

func (m *Metrics) RecordTaskStarted(taskName string) {
	m.taskStartedTotal.WithLabelValues(taskName).Inc()
	m.taskInProgress.WithLabelValues(taskName).Inc()
}

func (m *Metrics) RecordTaskSucceeded(taskName string, processingTime float64) {
	m.taskSucceededTotal.WithLabelValues(taskName).Inc()
	m.taskInProgress.WithLabelValues(taskName).Dec()
	m.taskProcessingTime.WithLabelValues(taskName).Observe(processingTime)
}

func (m *Metrics) RecordTaskFailed(taskName string, processingTime float64) {
	m.taskFailedTotal.WithLabelValues(taskName).Inc()
	m.taskInProgress.WithLabelValues(taskName).Dec()
	m.taskProcessingTime.WithLabelValues(taskName).Observe(processingTime)
}

func (m *Metrics) RecordTaskDrop(taskName, lastEvent string) {
	m.taskDropTotal.WithLabelValues(taskName, lastEvent).Inc()
}

func (m *Metrics) RecordTaskQueueLatency(taskName string, latency float64) {
	m.taskQueueLatency.WithLabelValues(taskName).Observe(latency)
}

func (m *Metrics) RecordTaskEndToEnd(taskName, status string, duration float64) {
	m.taskEndToEndDuration.WithLabelValues(taskName, status).Observe(duration)
}

var (
	globalRegistry *prometheus.Registry
)

func init() {
	// Initialize the global registry once during package initialization
	globalRegistry = prometheus.NewRegistry()

	// Register standard collectors
	globalRegistry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	globalRegistry.MustRegister(collectors.NewGoCollector())
}

// registerProbeMetrics adds probe-specific metrics to the global registry
func registerProbeMetrics() {
	// If we have a probe manager with active probes, gather their metrics
	if pm := NewProbeManager(Config); len(pm.Probes) > 0 {
		// Create application-level metrics once
		appMetrics := InitMetrics("celery_monitor")
		
		// Register application metrics with the global registry
		Log.Info().Msg("Registering common metrics at application level")
		
		// Task event counters
		globalRegistry.MustRegister(appMetrics.taskSentTotal)
		globalRegistry.MustRegister(appMetrics.taskReceivedTotal)
		globalRegistry.MustRegister(appMetrics.taskStartedTotal)
		globalRegistry.MustRegister(appMetrics.taskSucceededTotal)
		globalRegistry.MustRegister(appMetrics.taskFailedTotal)
		globalRegistry.MustRegister(appMetrics.taskDropTotal)
		
		// Task timing metrics
		globalRegistry.MustRegister(appMetrics.taskProcessingTime)
		globalRegistry.MustRegister(appMetrics.taskQueueLatency)
		globalRegistry.MustRegister(appMetrics.taskEndToEndDuration)
		
		// System metrics
		globalRegistry.MustRegister(appMetrics.taskInProgress)
		
		// Register probe info metrics for each probe
		for name, probe := range pm.Probes {
			if probe.Config.Enabled && probe.Metrics != nil {
				Log.Info().Str("probe", name).Msg("Adding probe info metric")
				
				// Add probe info metric only - other metrics are registered at the app level
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
		
		Log.Info().Int("probes", len(pm.Probes)).Msg("Registered metrics for all probes")
	}
}

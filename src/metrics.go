package main

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics contains all the Prometheus metrics for the application
type Metrics struct {
	// Task event metrics
	taskSentTotal      *prometheus.CounterVec
	taskReceivedTotal  *prometheus.CounterVec
	taskStartedTotal   *prometheus.CounterVec
	taskSucceededTotal *prometheus.CounterVec
	taskFailedTotal    *prometheus.CounterVec
	taskDropTotal      *prometheus.CounterVec

	// Task timing metrics
	taskProcessingTime   *prometheus.HistogramVec
	taskQueueLatency     *prometheus.HistogramVec
	taskEndToEndDuration *prometheus.HistogramVec

	// System metrics
	taskInProgress *prometheus.GaugeVec
	
	// Registry for all metrics
	registry *prometheus.Registry
}

var (
	// Global metrics instance
	AppMetrics *Metrics
	// Ensure initialization happens only once
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
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "sent_total",
					Help:      "Total number of celery tasks sent",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "queue"},
			),
			
			taskReceivedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "received_total",
					Help:      "Total number of celery tasks received",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),
			
			taskStartedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "started_total",
					Help:      "Total number of celery tasks started",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),
			
			taskSucceededTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "succeeded_total",
					Help:      "Total number of celery tasks succeeded",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),
			
			taskFailedTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "failed_total",
					Help:      "Total number of celery tasks failed",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),
			
			taskDropTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "drop_total",
					Help:      "Total number of celery tasks dropped",
					ConstLabels: commonLabels,
				},
				[]string{"task_name", "last_event"},
			),
			
			// Timing metrics
			taskProcessingTime: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "processing_seconds",
					Help:      "Time taken to process tasks",
					ConstLabels: commonLabels,
					Buckets:   prometheus.DefBuckets,
				},
				[]string{"task_name"},
			),
			
			taskQueueLatency: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "queue_latency_seconds",
					Help:      "Time spent in queue before processing",
					ConstLabels: commonLabels,
					Buckets:   prometheus.DefBuckets,
				},
				[]string{"task_name"},
			),
			
			taskEndToEndDuration: factory.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "end_to_end_seconds",
					Help:      "Total time from task sent to completion",
					ConstLabels: commonLabels,
					Buckets:   prometheus.DefBuckets,
				},
				[]string{"task_name", "status"},
			),
			
			// System metrics
			taskInProgress: factory.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: "celery",
					Subsystem: "tasks",
					Name:      "in_progress",
					Help:      "Number of tasks currently in progress",
					ConstLabels: commonLabels,
				},
				[]string{"task_name"},
			),
		}
		
		AppMetrics = m
	})
	
	return AppMetrics
}

// RecordTaskSent records a task sent event
func (m *Metrics) RecordTaskSent(taskName, queue string) {
	m.taskSentTotal.WithLabelValues(taskName, queue).Inc()
}

// RecordTaskReceived records a task received event
func (m *Metrics) RecordTaskReceived(taskName string) {
	m.taskReceivedTotal.WithLabelValues(taskName).Inc()
}

// RecordTaskStarted records a task started event
func (m *Metrics) RecordTaskStarted(taskName string) {
	m.taskStartedTotal.WithLabelValues(taskName).Inc()
	m.taskInProgress.WithLabelValues(taskName).Inc()
}

// RecordTaskSucceeded records a task succeeded event
func (m *Metrics) RecordTaskSucceeded(taskName string, processingTime float64) {
	m.taskSucceededTotal.WithLabelValues(taskName).Inc()
	m.taskInProgress.WithLabelValues(taskName).Dec()
	m.taskProcessingTime.WithLabelValues(taskName).Observe(processingTime)
}

// RecordTaskFailed records a task failed event
func (m *Metrics) RecordTaskFailed(taskName string, processingTime float64) {
	m.taskFailedTotal.WithLabelValues(taskName).Inc()
	m.taskInProgress.WithLabelValues(taskName).Dec()
	m.taskProcessingTime.WithLabelValues(taskName).Observe(processingTime)
}

// RecordTaskDrop records a task drop event
func (m *Metrics) RecordTaskDrop(taskName, lastEvent string) {
	m.taskDropTotal.WithLabelValues(taskName, lastEvent).Inc()
}

// RecordTaskQueueLatency records the time a task spent in the queue
func (m *Metrics) RecordTaskQueueLatency(taskName string, latency float64) {
	m.taskQueueLatency.WithLabelValues(taskName).Observe(latency)
}

// RecordTaskEndToEnd records the total time from task sent to completion
func (m *Metrics) RecordTaskEndToEnd(taskName, status string, duration float64) {
	m.taskEndToEndDuration.WithLabelValues(taskName, status).Observe(duration)
}

var (
	// globalRegistry is the shared metric registry for the application
	globalRegistry *prometheus.Registry
)

func init() {
	// Initialize the global registry once during package initialization
	globalRegistry = prometheus.NewRegistry()

	// Register standard collectors
	globalRegistry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	globalRegistry.MustRegister(prometheus.NewGoCollector())
}

// registerProbeMetrics adds probe-specific metrics to the global registry
func registerProbeMetrics() {
	// If we have a probe manager with active probes, gather their metrics
	if pm := NewProbeManager(Config); len(pm.Probes) > 0 {
		// For each probe, register its metrics with the global registry
		for name, probe := range pm.Probes {
			if probe.Config.Enabled && probe.Metrics != nil && probe.Metrics.registry != nil {
				Log.Info().Str("probe", name).Msg("Adding metrics from probe to global registry")
				
				// Add probe info metric
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
}

// RunMetricsServer runs the metrics server with a registry that collects metrics from all probes
func RunMetricsServer() {
	// Register probe-specific metrics
	registerProbeMetrics()

	// Create a new HTTP handler for metrics
	handler := promhttp.HandlerFor(globalRegistry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})

	// Set up metrics server endpoint with reasonable timeouts
	server := &http.Server{
		Addr:         ":" + strconv.Itoa(int(Config.MetricServerPort)),
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		Log.Info().Int("port", int(Config.MetricServerPort)).Msg("Starting metric server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			Log.Fatal().Err(err).Msg("Failed to start metrics server")
		}
	}()
}

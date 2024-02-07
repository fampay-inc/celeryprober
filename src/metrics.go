package main

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labelNames)
	if err := prometheus.DefaultRegisterer.Register(c); err != nil {
		// If the counter is already registered, return the existing counter
		if collector, ok := err.(prometheus.AlreadyRegisteredError); ok {
			c = collector.ExistingCollector.(*prometheus.CounterVec)
		} else {
			// TODO: Integrate Sentry
			// sentry.WithScope(func(scope *sentry.Scope) {
			// 	scope.SetTag("metrics", opts.Name)
			// 	sentry.CaptureException(err)
			// })
		}
	}

	return c
}

// RunMetricsServer runs the metrics server.
func RunMetricsServer() {
	// set metrics server endpoint
	server := &http.Server{Addr: ":" + strconv.Itoa(int(Config.MetricServerPort)), Handler: promhttp.Handler()}

	go func() {
		Logger.Println("Starting metric server at :2112")
		// if error, log error and restart metrics server
		if err := server.ListenAndServe(); err != nil {
			// TODO: Integrate Sentry
			// sentry.CaptureException(err)
			Logger.Fatal(err)
		}
	}()
}

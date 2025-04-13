package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
)

var (
	Config *GlobalConfig
	Logger *log.Logger
)

// These functions are now handled by the Probe struct

func waitForInterrupt(ctx context.Context) {
	<-ctx.Done()
}

func gracefulShutdown(ctx context.Context, pm *ProbeManager) {
	waitForInterrupt(ctx)
	Log.Info().Msg("Shutting down all probes...")

	// Shutdown all probes
	pm.Shutdown(ctx)

	Log.Info().Msg("Service stopped gracefully")
}

// Metrics initialization is now handled by each Probe

func server() {
	Log.Info().Msg("Starting server...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create probe manager
	pm := NewProbeManager(Config)

	// Start all probes and count healthy ones
	var healthyProbeCount int
	pm.Start(ctx)
	
	// Check which probes are healthy
	for name, probe := range pm.Probes {
		if probe.IsHealthy {
			healthyProbeCount++
		} else {
			LogWarnEvent(name).Msg("Probe is not healthy - monitoring will be limited")
		}
	}
	
	Log.Info().Int("total_probes", len(pm.Probes)).Int("healthy_probes", healthyProbeCount).Msg("Probe initialization complete")

	// Start REST and metrics servers
	RunRESTServer()
	RunMetricsServer()

	// Wait for shutdown signal
	gracefulShutdown(ctx, pm)
}

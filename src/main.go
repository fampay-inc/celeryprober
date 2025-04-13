package main

func main() {
	// Initialize global configuration
	Config = NewConfig()
	
	// Initialize structured logger
	InitLogger()
	Log.Info().Str("app", "celery-monitor").Msg("Starting application")

	// Run in server or cron mode based on configuration
	switch Config.Mode {
	case Server:
		server()
	case Cron:
		cron()
	}
}

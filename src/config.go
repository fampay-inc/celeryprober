package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v10"
)

type ApplicationMode string

const (
	Server ApplicationMode = "server"
	Cron   ApplicationMode = "cron"
)

// GlobalConfig contains application-wide settings
type GlobalConfig struct {
	RESTServerPort   int             `env:"REST_SERVER_PORT" envDefault:"3000" json:"rest_server_port"`
	MetricServerPort int             `env:"METRIC_SERVER_PORT" envDefault:"2112" json:"metric_server_port"`
	Mode             ApplicationMode `env:"APPLICATION_MODE" envDefault:"server" json:"mode"`
	ConfigFile       string          `env:"CONFIG_FILE" json:"-"` // Path to JSON config file for probes
	SlackAccessToken string          `env:"SLACK_ACCESS_TOKEN" envDefault:"" json:"slack_access_token"`
	SlackChannelId   string          `env:"SLACK_CHANNEL_ID" envDefault:"" json:"slack_channel_id"`

	// List of probe configurations
	Probes []*ProbeConfig `json:"probes"`
}

// ProbeConfig contains configuration for a single celery monitoring probe
type ProbeConfig struct {
	// Probe identification
	Name        string `json:"name" validate:"required"` // Name of the probe, used in metrics and logs
	Enabled     bool   `json:"enabled"`                    // Whether this probe is enabled
	Description string `json:"description,omitempty"`      // Optional description

	// Redis configuration
	CeleryRedisBrokerURL string `json:"celery_redis_broker_url" validate:"required"` // Redis broker URL for Celery
	StaleTaskSetKey     string `json:"stale_task_set_key" validate:"required"`       // Redis key for storing stale tasks
	
	// Generated fields
	TaskEventChannels []string `json:"-"` // Generated Celery event channels to subscribe to

	// Task processing configuration
	StaleTaskCallbackContextTimeoutInSec int      `json:"stale_task_callback_context_timeout_sec" validate:"required"` // Timeout for stale task callbacks
	StaleTaskCallbackDelayDurationInMin  int      `json:"stale_task_callback_delay_duration_min" validate:"required"` // Delay before considering a task stale
	BlacklistedTaskNames                 []string `json:"blacklisted_task_names,omitempty"`                           // Task names to ignore

	// Computed fields (not from JSON)
	StaleTaskCallbackContextTimeout time.Duration `json:"-"`
	StaleTaskCallbackDelayDuration  time.Duration `json:"-"`
}

// DefaultProbeConfig creates a default probe configuration
func DefaultProbeConfig() *ProbeConfig {
	return &ProbeConfig{
		Name:                                 "default",
		Enabled:                              true,
		Description:                          "Default celery monitor probe",
		CeleryRedisBrokerURL:                 "redis://localhost:6379/6",
		StaleTaskSetKey:                      "stale_tasks",
		StaleTaskCallbackContextTimeoutInSec: 5,
		StaleTaskCallbackDelayDurationInMin:  15,
		BlacklistedTaskNames:                 []string{},
	}
}

// Initialize computes derived fields from configuration values
func (pc *ProbeConfig) Initialize() {
	pc.StaleTaskCallbackContextTimeout = time.Duration(pc.StaleTaskCallbackContextTimeoutInSec) * time.Second
	pc.StaleTaskCallbackDelayDuration = time.Duration(pc.StaleTaskCallbackDelayDurationInMin) * time.Minute
	
	// Generate TaskEventChannels from the CeleryRedisBrokerURL
	pc.TaskEventChannels = generateTaskEventChannels(pc.CeleryRedisBrokerURL)
	// We can't reliably log here because Logger might not be initialized yet
}

// NewConfig creates a new configuration from environment variables and optional config file
func NewConfig() *GlobalConfig {
	// First load environment variables
	config := &GlobalConfig{}
	if err := env.Parse(config); err != nil {
		Logger.Fatal("Unable to parse configuration from environment variables")
	}

	// If config file is specified, load probe configurations from it
	if config.ConfigFile != "" {
		if err := loadConfigFile(config); err != nil {
			Logger.Fatalf("Error loading config file: %v", err)
		}
	} else {
		// No config file, create a single default probe from environment variables
		envProbe := loadProbeFromEnv()
		config.Probes = []*ProbeConfig{envProbe}
	}

	// Initialize all probes
	for _, probe := range config.Probes {
		if probe.Enabled {
			probe.Initialize()
		}
	}

	return config
}

// loadConfigFile loads only probe configurations from a JSON file,
// leaving other global settings to come from environment variables or defaults
func loadConfigFile(config *GlobalConfig) error {
	file, err := os.Open(config.ConfigFile)
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Use a temporary struct that only contains the probes field
	type ProbesConfig struct {
		Probes []*ProbeConfig `json:"probes"`
	}
	
	tempConfig := &ProbesConfig{}
	if err := json.Unmarshal(bytes, tempConfig); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Copy the probes to the global config
	config.Probes = tempConfig.Probes

	// Validate that we have at least one probe
	if len(config.Probes) == 0 {
		config.Probes = []*ProbeConfig{DefaultProbeConfig()}
	}

	return nil
}

// loadProbeFromEnv creates a probe configuration from environment variables
// for backward compatibility
func loadProbeFromEnv() *ProbeConfig {
	probe := &ProbeConfig{}

	// Use env.Parse to load environment variables into the probe
	type envProbe struct {
		ServiceName                          string   `env:"SERVICE_NAME" envDefault:"celery-monitor"`
		CeleryRedisBrokerURL                 string   `env:"CELERY_REDIS_BROKER_URL" envDefault:""`
		LegacyRedisURL                       string   `env:"REDIS_URL" envDefault:"redis://localhost:6379/6"`
		StaleTaskSetKey                      string   `env:"STALE_TASK_SET_KEY" envDefault:"stale_tasks"`
		StaleTaskCallbackContextTimeoutInSec int      `env:"STALE_TASK_CALLBACK_CONTEXT_TIMEOUT_IN_SEC" envDefault:"5"`
		StaleTaskCallbackDelayDurationInMin  int      `env:"STALE_TASK_CALLBACK_DELAY_DURATION_IN_MIN" envDefault:"15"`
		BlacklistedTaskNames                 []string `env:"BLACKLISTED_TASK_NAMES" envSeparator:","`
	}

	ep := envProbe{}
	if err := env.Parse(&ep); err != nil {
		Logger.Fatal("Unable to parse probe configuration from environment variables")
	}

	// Transfer values to the probe
	probe.Name = ep.ServiceName
	probe.Enabled = true
	probe.Description = fmt.Sprintf("Probe for %s", ep.ServiceName)
	
	// Use the new CELERY_REDIS_BROKER_URL if provided, otherwise fall back to REDIS_URL
	if ep.CeleryRedisBrokerURL != "" {
		probe.CeleryRedisBrokerURL = ep.CeleryRedisBrokerURL
	} else {
		probe.CeleryRedisBrokerURL = ep.LegacyRedisURL
	}
	
	probe.StaleTaskSetKey = ep.StaleTaskSetKey
	probe.StaleTaskCallbackContextTimeoutInSec = ep.StaleTaskCallbackContextTimeoutInSec
	probe.StaleTaskCallbackDelayDurationInMin = ep.StaleTaskCallbackDelayDurationInMin
	probe.BlacklistedTaskNames = ep.BlacklistedTaskNames
	
	// TaskEventChannels are now generated in Initialize() based on the Redis URL

	return probe
}

// generateTaskEventChannels extracts the database number from the Redis URL
// and generates the standard Celery event channel names
func generateTaskEventChannels(redisURL string) []string {
	// Parse the Redis URL to extract the database number
	dbNumber := extractDBNumberFromRedisURL(redisURL)
	
	// Standard Celery event types
	eventTypes := []string{
		"task.sent",
		"task.received",
		"task.started",
		"task.succeeded",
		"task.failed",
	}
	
	// Generate the channel names with the extracted DB number
	channels := make([]string, len(eventTypes))
	for i, eventType := range eventTypes {
		channels[i] = fmt.Sprintf("/%s.celeryev/%s", dbNumber, eventType)
	}
	
	return channels
}

// extractDBNumberFromRedisURL extracts the database number from a Redis URL
// Redis URLs have the format: redis://host:port/dbNumber
// If no database is specified, it defaults to 0
func extractDBNumberFromRedisURL(redisURL string) string {
	// Default database number
	defaultDB := "0"
	
	// If URL is empty, return default
	if redisURL == "" {
		return defaultDB
	}
	
	// Parse the URL
	parsedURL, err := url.Parse(redisURL)
	if err != nil {
		// Can't use Log here due to initialization order
fmt.Printf("Failed to parse Redis URL: %s - %v\n", redisURL, err)
return defaultDB
}

// Extract the path which contains the DB number
path := parsedURL.Path
if path == "" || path == "/" {
return defaultDB
}

// Remove leading slash if present
dbStr := strings.TrimPrefix(path, "/")

// Validate that it's a number
	_, err = strconv.Atoi(dbStr)
	if err != nil {
		// Can't use Log here due to initialization order
fmt.Printf("Invalid database number in Redis URL: %s\n", dbStr)
return defaultDB
}

return dbStr
}

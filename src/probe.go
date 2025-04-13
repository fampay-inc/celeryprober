package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Probe represents a single celery monitoring probe instance
type Probe struct {
	Config *ProbeConfig

	RedisClient *redis.Client
	PubSub      *redis.PubSub

	TaskStatsMap      taskStatsMap
	TaskStatsMapMutex sync.RWMutex
	StaleTaskChannel  chan *StaleTask

	WaitGroup struct {
		PubSubChannelConsumer    sync.WaitGroup
		StaleTaskChannelConsumer sync.WaitGroup
		Callback                 sync.WaitGroup
	}

	Metrics   *Metrics
	IsHealthy bool
}

// ProbeManager manages multiple probes
type ProbeManager struct {
	Probes map[string]*Probe
	Config *GlobalConfig
}

// NewProbeManager creates a new probe manager
func NewProbeManager(config *GlobalConfig) *ProbeManager {
	pm := &ProbeManager{
		Probes: make(map[string]*Probe),
		Config: config,
	}

	// Create probes from configuration
	for _, probeConfig := range config.Probes {
		if probeConfig.Enabled {
			probe := NewProbe(probeConfig)
			pm.Probes[probeConfig.Name] = probe
		}
	}

	return pm
}

// NewProbe creates a new probe from configuration
func NewProbe(config *ProbeConfig) *Probe {
	probe := &Probe{
		Config:           config,
		TaskStatsMap:     make(taskStatsMap),
		StaleTaskChannel: make(chan *StaleTask),
	}

	// Initialize metrics for this probe
	probe.Metrics = InitMetrics()

	return probe
}

// Start initializes and starts all probes
func (pm *ProbeManager) Start(ctx context.Context) {
	for name, probe := range pm.Probes {
		LogEvent(name).Msg("Starting probe")
		// Run synchronously so we can check IsHealthy status
		probe.Start(ctx)
	}
}

// Shutdown gracefully stops all probes
func (pm *ProbeManager) Shutdown(ctx context.Context) {
	for name, probe := range pm.Probes {
		LogEvent(name).Msg("Shutting down probe")
		probe.Shutdown(ctx)
	}
}

// Start initializes and starts a probe
func (p *Probe) Start(ctx context.Context) {
	p.initializeRedis()
	p.initializePubSub(ctx)

	// Only initialize listeners if Redis connection is healthy
	if p.IsHealthy {
		p.initializeListeners()
	} else {
		LogWarnEvent(p.Config.Name).Msg("Skipping listener initialization due to unhealthy Redis connection")
	}
}

// Shutdown gracefully stops a probe
func (p *Probe) Shutdown(ctx context.Context) {
	// Skip if probe wasn't properly initialized or is unhealthy
	if p.PubSub == nil || !p.IsHealthy {
		LogEvent(p.Config.Name).Msg("Skipping shutdown for unhealthy probe")
		return
	}

	// Close PubSub and wait for consumer to finish
	p.PubSub.Close()
	p.WaitGroup.PubSubChannelConsumer.Wait()

	// Wait for scheduled callbacks to be executed
	LogEvent(p.Config.Name).Msg("Waiting for scheduled callbacks to complete")
	p.WaitGroup.Callback.Wait()

	// Close stale task channel and wait for consumer to finish
	LogEvent(p.Config.Name).Msg("Waiting for stale tasks to be processed")
	close(p.StaleTaskChannel)
	p.WaitGroup.StaleTaskChannelConsumer.Wait()

	// Close Redis client
	p.RedisClient.Close()

	LogEvent(p.Config.Name).Msg("Probe stopped gracefully")
}

// initializeRedis initializes the Redis client for this probe
func (p *Probe) initializeRedis() {
	redisClientOptions, err := redis.ParseURL(p.Config.CeleryRedisBrokerURL)
	if err != nil {
		Logger.Fatalf("Cannot parse Redis broker URL for probe %s: %s", p.Config.Name, p.Config.CeleryRedisBrokerURL)
	}

	redisClientOptions.ClientName = p.Config.Name
	p.RedisClient = redis.NewClient(redisClientOptions)
}

// initializePubSub initializes the Redis PubSub for this probe
func (p *Probe) initializePubSub(ctx context.Context) {
	LogEvent(p.Config.Name).Msg("Starting PubSub subscriber")

	p.PubSub = p.RedisClient.Subscribe(ctx, p.Config.TaskEventChannels...)
	if err := p.PubSub.Ping(ctx, "ping"); err != nil {
		LogErrorEvent(p.Config.Name, err).Str("action", "ping_pubsub").Msg("Failed to connect to Redis")
		p.IsHealthy = false
		return
	}

	p.IsHealthy = true
	LogEvent(p.Config.Name).Strs("channels", p.Config.TaskEventChannels).Msg("Subscribed to Redis channels")
}

// initializeListeners initializes the listeners for this probe
func (p *Probe) initializeListeners() {
	p.WaitGroup.StaleTaskChannelConsumer.Add(1)
	go p.consumeStaleTaskChannel()
	p.WaitGroup.PubSubChannelConsumer.Add(1)
	go p.consumePubSubChannel()
}

// consumeStaleTaskChannel consumes stale tasks from the channel
func (p *Probe) consumeStaleTaskChannel() {
	defer p.WaitGroup.StaleTaskChannelConsumer.Done()
	LogEvent(p.Config.Name).Msg("Stale task channel listener ready")

	for stale_task := range p.StaleTaskChannel {
		ctx, cancel := context.WithTimeout(context.Background(), p.Config.StaleTaskCallbackContextTimeout)
		taskID := stale_task.TaskId.String()
		stats_json, _ := json.Marshal(stale_task.Stats)

		// Log stale task detection with structured data
		LogEvent(p.Config.Name).
			Str("event", "stale_task_detected").
			Str("task_id", taskID).
			Str("task_name", stale_task.Stats.Name).
			Str("last_event", string(stale_task.Stats.GetLatestEvent())).
			Float64("timestamp", stale_task.Stats.LatestEventTimestamp).
			Msg("Task dropped")

		// Store in Redis before sending notification
		_, err := p.RedisClient.HSet(ctx, p.Config.StaleTaskSetKey, taskID, stats_json).Result()
		if err != nil {
			LogErrorEvent(p.Config.Name, err).
				Str("task_id", taskID).
				Msg("Failed to store stale task in Redis")
		} else {
			LogEvent(p.Config.Name).
				Str("action", "store_stale_task").
				Str("task_id", taskID).
				Msg("Stored stale task in Redis")
		}

		// Prepare a more concise and clear slack notification
		taskName := stale_task.Stats.Name
		lastEvent := string(stale_task.Stats.GetLatestEvent())
		slackMsg := fmt.Sprintf(
			"*Alert:* Task dropped\n*Service:* `%s`\n*Task:* `%s`\n*ID:* `%s`\n*Last Event:* `%s`",
			p.Config.Name,
			taskName,
			taskID,
			lastEvent,
		)

		// Send notification to Slack
		status_code, _, err := SendMessageToSlackChannel(slackMsg)
		if err != nil {
			LogErrorEvent(p.Config.Name, err).
				Str("task_id", taskID).
				Msg("Failed to send Slack notification")
		} else {
			LogEvent(p.Config.Name).
				Str("action", "slack_notification").
				Int("status_code", status_code).
				Str("task_id", taskID).
				Msg("Sent Slack notification")
		}

		cancel()
		p.TaskStatsMap.Delete(stale_task.TaskId)
	}

	LogEvent(p.Config.Name).Msg("Stale task channel closed")
}

// consumePubSubChannel consumes Celery task events from Redis PubSub
func (p *Probe) consumePubSubChannel() {
	defer p.WaitGroup.PubSubChannelConsumer.Done()
	LogEvent(p.Config.Name).Msg("PubSub channel listener ready")

	for msg := range p.PubSub.Channel() {
		raw_event := RawEvent{}
		err := json.Unmarshal([]byte(msg.Payload), &raw_event)
		if err != nil {
			LogErrorEvent(p.Config.Name, err).
				Str("channel", msg.Channel).
				Str("payload", msg.Payload).
				Msg("Failed to parse message payload")
			continue
		}

		event_json_bytearray, err := base64.StdEncoding.DecodeString(raw_event.Body)
		if err != nil {
			LogErrorEvent(p.Config.Name, err).
				Str("channel", msg.Channel).
				Str("body", raw_event.Body).
				Msg("Failed to decode base64 event string")
			continue
		}

		event := GenerateEventObject(msg.Channel)
		if event == nil {
			LogEvent(p.Config.Name).
				Str("level", "warn").
				Str("channel", msg.Channel).
				Str("payload", string(event_json_bytearray)).
				Msg("Invalid channel configured")
			continue
		}

		err = json.Unmarshal(event_json_bytearray, event)
		if err != nil {
			LogErrorEvent(p.Config.Name, err).
				Str("channel", msg.Channel).
				Str("payload", string(event_json_bytearray)).
				Msg("Failed to parse event JSON")
			continue
		}

		LogEvent(p.Config.Name).
			Str("event_type", string(event.Type())).
			Str("task_id", event.ID().String()).
			Msg("Processed event")

		var task_start_delay time.Duration
		if event.Type() == TaskEventTypeSent {
			task_sent_event := event.(*TaskSent)
			if strings.HasSuffix(task_sent_event.Queue, "dlq") {
				// Don't want to process DLQ events
				continue
			}

			// Emitting task_sent counter metric
			p.Metrics.RecordTaskSent(task_sent_event.Name, task_sent_event.Queue, p.Config.Name)

			task_start_delay, err = task_sent_event.GetTaskStartDelayDuration()
			if err != nil {
				Logger.Printf("Unable to parse eta time for probe %s due to error: %s", p.Config.Name, err)
			}
		}

		task_id := event.ID()
		stats, ok := p.TaskStatsMap.Read(task_id)
		if !ok {
			stats = NewTaskStats()
			// Set the probe name to ensure correct metrics labeling
			stats.ProbeName = p.Config.Name
			p.WaitGroup.Callback.Add(1)
			stats.callBackTimer = time.AfterFunc(p.Config.StaleTaskCallbackDelayDuration+task_start_delay, func() {
				defer p.WaitGroup.Callback.Done()

				var is_blacklisted_task bool
				for _, task_name := range p.Config.BlacklistedTaskNames {
					if task_name == stats.Name {
						is_blacklisted_task = true
					}
				}

				if len(stats.Runtimes) > 0 || stats.Name == "" || is_blacklisted_task {
					// Terminal event received or
					// No sent event received for this task
					// or blacklisted task
					// Hence cannot be considered as a task drop case
					p.TaskStatsMap.Delete(task_id)
					return
				}

				stats_json, _ := json.Marshal(stats)
				Logger.Printf("Task identified as stale in probe %s, TaskId: %s, Stats: %s",
					p.Config.Name, task_id, stats_json)

				if stats.Name != "" {
					p.Metrics.RecordTaskDrop(stats.Name, string(stats.GetLatestEvent()), p.Config.Name)
				}

				p.StaleTaskChannel <- &StaleTask{
					TaskId: task_id,
					Stats:  stats,
				}
			})
			p.TaskStatsMap.Write(task_id, stats)
		} else if event.Type() == TaskEventTypeSent {
			/*
				Resets the callback timer if:
				- task-sent event is received out of order
				- task-sent event is received in case of task retry
			*/
			stats.callBackTimer.Reset(p.Config.StaleTaskCallbackDelayDuration + task_start_delay)
		}

		// Process the event (update TaskStats)
		event.Process(stats)

		// Record metrics based on the event type
		if stats.Name != "" {
			switch event.Type() {
			case TaskEventTypeReceived:
				p.Metrics.RecordTaskReceived(stats.Name, p.Config.Name)
				// Calculate and record queue latency if we have sent timestamps
				if len(stats.SentTimestamps) > 0 && len(stats.ReceivedTimestamps) > 0 {
					latency := stats.ReceivedTimestamps[len(stats.ReceivedTimestamps)-1] - stats.SentTimestamps[0]
					if latency > 0 {
						p.Metrics.RecordTaskQueueLatency(stats.Name, latency, p.Config.Name)
					}
				}

			case TaskEventTypeStarted:
				p.Metrics.RecordTaskStarted(stats.Name, p.Config.Name)

			case TaskEventTypeSucceeded:
				// Get the runtime from the most recent success
				if len(stats.Runtimes) > 0 {
					runtime := stats.Runtimes[len(stats.Runtimes)-1]
					p.Metrics.RecordTaskSucceeded(stats.Name, runtime, p.Config.Name)

					// Calculate and record end-to-end duration
					if len(stats.SentTimestamps) > 0 && len(stats.SucceededTimestamps) > 0 {
						duration := stats.SucceededTimestamps[len(stats.SucceededTimestamps)-1] - stats.SentTimestamps[0]
						if duration > 0 {
							p.Metrics.RecordTaskEndToEnd(stats.Name, "succeeded", duration, p.Config.Name)
						}
					}
				}

			case TaskEventTypeFailed:
				// Get the runtime from the most recent failure
				if len(stats.Runtimes) > 0 {
					runtime := stats.Runtimes[len(stats.Runtimes)-1]
					p.Metrics.RecordTaskFailed(stats.Name, runtime, p.Config.Name)

					// Calculate and record end-to-end duration
					if len(stats.SentTimestamps) > 0 && len(stats.FailedTimestamps) > 0 {
						duration := stats.FailedTimestamps[len(stats.FailedTimestamps)-1] - stats.SentTimestamps[0]
						if duration > 0 {
							p.Metrics.RecordTaskEndToEnd(stats.Name, "failed", duration, p.Config.Name)
						}
					}
				}
			}
		}

		// Check if task lifecycle is complete
		if stats.IsTaskLifecycleComplete() {
			if timer_stopped := stats.callBackTimer.Stop(); timer_stopped {
				p.WaitGroup.Callback.Done()
			}
			p.TaskStatsMap.Delete(task_id)
			Logger.Printf("Removed Task (ID: %s) from memory for probe %s since lifecycle completion reached",
				task_id, p.Config.Name)
		}
	}

	Logger.Printf("PubSub channel closed for probe: %s", p.Config.Name)
}

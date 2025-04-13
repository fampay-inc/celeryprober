package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type DailyReport struct {
	TotalDroppedCount uint64            `json:"total_dropped_count"`
	DroppedTasks      map[string]string `json:"dropped_tasks"`
}

func NewDailyReport() *DailyReport {
	return &DailyReport{
		TotalDroppedCount: 0,
		DroppedTasks:      map[string]string{},
	}
}

func (report *DailyReport) AddDroppedTask(task_id, stats_json string) {
	report.DroppedTasks[task_id] = stats_json
	report.TotalDroppedCount++
}

// This method is no longer used as the functionality is now in processCronForProbe
func (report *DailyReport) SentToSlack() {
	// Keeping this method for backward compatibility
	report_json, err := json.Marshal(report)
	if err != nil {
		Log.Fatal().Err(err).Msg("Unable to marshal DailyReport struct to JSON")
	}

	Log.Debug().RawJSON("report", report_json).Msg("Generated report")

	status_code, body, err := SendFileToSlackChannel("Task drop report", "task-drop-report.json", report_json)

	if err != nil {
		Log.Error().Err(err).Msg("Failed to upload file to Slack")
	} else {
		Log.Info().Int("status_code", status_code).Str("body", string(body)).Msg("Sent report file to Slack")
	}
}

// processCronForProbe processes the cron job for a single probe
func processCronForProbe(ctx context.Context, probe *ProbeConfig) {
	logEvent := LogEvent(probe.Name)
	logEvent.Str("action", "cron_job").Msg("Processing cron job")

	// Initialize Redis client for this probe
	redisClientOptions, err := redis.ParseURL(probe.CeleryRedisBrokerURL)
	if err != nil {
		LogErrorEvent(probe.Name, err).
			Str("redis_url", probe.CeleryRedisBrokerURL).
			Msg("Failed to parse Redis URL")
		return
	}

	redisClientOptions.ClientName = probe.Name
	redisClient := redis.NewClient(redisClientOptions)
	defer redisClient.Close()

	// Get all stale tasks for this probe
	result, err := redisClient.HGetAll(ctx, probe.StaleTaskSetKey).Result()
	if err != nil {
		LogErrorEvent(probe.Name, err).
			Str("key", probe.StaleTaskSetKey).
			Msg("Failed to fetch stale tasks from Redis")
		return
	}

	logEvent.Int("stale_task_count", len(result)).Msg("Retrieved stale tasks")

	// Skip further processing if no tasks found
	if len(result) == 0 {
		logEvent.Msg("No stale tasks found")
		return
	}

	// Process each task and prepare data for report
	taskSummaries := []map[string]interface{}{}
	task_id_list := []string{}
	formattedTasksForSlack := []string{}

	for task_id, stats_json := range result {
		stats := &TaskStats{}
		if err := json.Unmarshal([]byte(stats_json), stats); err != nil {
			LogErrorEvent(probe.Name, err).
				Str("task_id", task_id).
				Msg("Failed to parse task stats")
			continue
		}

		// Create structured data for logging
		taskSummary := map[string]interface{}{
			"task_id":   task_id,
			"name":      stats.Name,
			"last_event": string(stats.GetLatestEvent()),
		}
		taskSummaries = append(taskSummaries, taskSummary)
		task_id_list = append(task_id_list, task_id)

		// Format task for Slack message
		shortTaskId := task_id
		if len(task_id) > 8 {
			shortTaskId = task_id[:8] + "..."
		}
		formattedTasksForSlack = append(formattedTasksForSlack, fmt.Sprintf(
			"• *Task:* `%s` - `%s`\n  *ID:* `%s`\n  *Last Event:* `%s`",
			stats.Name,
			string(stats.GetLatestEvent()),
			shortTaskId,
			time.Unix(int64(stats.LatestEventTimestamp), 0).Format("01/02 15:04:05"),
		))
	}

	// Send improved Slack message if there are tasks
	if len(task_id_list) > 0 {
		// Format current time for report
		currentTime := fmt.Sprintf("%s", time.Now().Format(time.RFC1123))

		// Create concise and clear Slack message
		slackMsg := fmt.Sprintf("*Stale Tasks Report: %s*\n\n", probe.Name)
		slackMsg += fmt.Sprintf("*Time:* %s\n", currentTime)
		slackMsg += fmt.Sprintf("*Status:* %d stale task(s) detected\n\n", len(task_id_list))
		
		// Add formatted tasks (maximum 10 to avoid large messages)
		maxTasksToShow := 10
		tasksToShow := len(formattedTasksForSlack)
		if tasksToShow > maxTasksToShow {
			tasksToShow = maxTasksToShow
			slackMsg += fmt.Sprintf("Showing %d of %d tasks:\n\n", tasksToShow, len(formattedTasksForSlack))
		}
		
		for i := 0; i < tasksToShow; i++ {
			slackMsg += formattedTasksForSlack[i] + "\n\n"
		}
		
		// Add footer with action advice
		slackMsg += "_Please investigate these tasks and take appropriate action._"

		// Send the message to Slack
		status_code, _, err := SendMessageToSlackChannel(slackMsg)
		if err != nil {
			LogErrorEvent(probe.Name, err).Msg("Failed to send Slack report")
		} else {
			logEvent.Int("status_code", status_code).Int("task_count", len(task_id_list)).Msg("Sent stale tasks report to Slack")
		}

		// Also upload the full report as JSON for detailed analysis
		report := NewDailyReport()
		for task_id, stats_json := range result {
			report.AddDroppedTask(task_id, stats_json)
		}

		report_json, err := json.Marshal(report)
		if err != nil {
			LogErrorEvent(probe.Name, err).Msg("Failed to marshal report to JSON")
		} else {
			_, _, err := SendFileToSlackChannel(
				fmt.Sprintf("Detailed report for %s", probe.Name),
				fmt.Sprintf("task-drop-report-%s.json", probe.Name),
				report_json,
			)
			if err != nil {
				LogErrorEvent(probe.Name, err).Msg("Failed to upload report file to Slack")
			} else {
				logEvent.Msg("Uploaded detailed report file to Slack")
			}
		}
	}

	// Delete processed tasks
	if len(task_id_list) > 0 {
		deleted_key_count, err := redisClient.HDel(ctx, probe.StaleTaskSetKey, task_id_list...).Result()
		if err != nil {
			LogErrorEvent(probe.Name, err).Msg("Failed to delete task objects from Redis")
		} else {
			logEvent.Int("deleted_count", int(deleted_key_count)).Msg("Cleaned up stale tasks from Redis")
		}
	}
}

func cron() {
	ctx := context.Background()
	
	// Check for required Slack configuration
	if Config.SlackAccessToken == "" || Config.SlackChannelId == "" {
		Log.Fatal().Msg("Slack Access Token and Channel ID are required for cron mode")
	}

	// Create probe manager to get probe configurations
	pm := NewProbeManager(Config)

	// Check if we have any enabled probes
	var enabledProbeCount int
	for _, probe := range pm.Config.Probes {
		if probe.Enabled {
			enabledProbeCount++
		}
	}

	if enabledProbeCount == 0 {
		Log.Warn().Msg("No enabled probes found. Exiting cron mode")
		return
	}

	Log.Info().Int("probe_count", enabledProbeCount).Msg("Starting cron jobs")

	// Process each enabled probe
	for _, probe := range pm.Config.Probes {
		if probe.Enabled {
			processCronForProbe(ctx, probe)
		}
	}
}

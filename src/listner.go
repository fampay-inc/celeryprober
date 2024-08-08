package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func consumeStaleTaskChannel() {
	defer WaitGroup.StaleTaskChannelConsumer.Done()
	Logger.Println("Stale task channel listener ready")

	for stale_task := range StaleTaskChannel {
		ctx, cancel := context.WithTimeout(context.Background(), Config.StaleTaskCallbackContextTimeout)

		stats_json, _ := json.Marshal(stale_task.Stats)

		status_code, body, err := SendMessageToSlackChannel(fmt.Sprintf(
			"Stale task detected\nTask ID: `%s`\nStats:\n```%s```",
			stale_task.TaskId.String(),
			strings.ReplaceAll(string(stats_json), "\\", ""),
		))
		if err != nil {
			Logger.Printf("Unable to send slack message for stale task (ID: %s): %s", stale_task.TaskId.String(), err)
		} else {
			Logger.Printf("Slack Chat PostMessage API Response Status Code: %d, Response Body: %s", status_code, body)
		}

		_, err = RedisClient.HSet(ctx, Config.StaleTaskSetKey, stale_task.TaskId.String(), stats_json).Result()
		if err != nil {
			Logger.Printf("Unable to send stale task (ID: %s) to Redis due to error: %s", stale_task.TaskId.String(), err)
		} else {
			Logger.Printf("Sent stale task (ID: %s) to Redis successfully", stale_task.TaskId)
		}

		cancel()

		TaskStatsMap.Delete(stale_task.TaskId)
	}

	Logger.Println("Stale task channel closed")
}

func consumePubSubChannel() {
	defer WaitGroup.PubSubChannelConsumer.Done()
	Logger.Println("PubSub channel listener ready")

	for msg := range PubSub.Channel() {
		raw_event := RawEvent{}
		err := json.Unmarshal([]byte(msg.Payload), &raw_event)
		if err != nil {
			Logger.Printf("Unable to parse message payload: %s", msg.Payload)
			continue
		}

		event_json_bytearray, err := base64.StdEncoding.DecodeString(raw_event.Body)
		if err != nil {
			Logger.Printf("Unable to decode base64 event string: %s", raw_event.Body)
			continue
		}

		event := GenerateEventObject(msg.Channel)
		if event == nil {
			Logger.Printf("Invalid channel configured, Channel name: %s, payload: %s", msg.Channel, event_json_bytearray)
			continue
		}

		err = json.Unmarshal(event_json_bytearray, event)
		if err != nil {
			Logger.Printf("Unable to parse base64 decoded payload: %s", event_json_bytearray)
			continue
		}

		Logger.Printf("Successfully parsed event: %s", event_json_bytearray)

		var task_start_delay time.Duration
		if event.Type() == TaskEventTypeSent {
			task_sent_event := event.(*TaskSent)
			if strings.HasSuffix(task_sent_event.Queue, "dlq") {
				// Don't want to process DLQ events
				continue
			}

			// Emitting task_sent counter metric
			NewCounterVec(prometheus.CounterOpts{
				Name: "celery_task_sent_total",
				Help: "total number of celery task_sent events",
			}, []string{"name"}).WithLabelValues(task_sent_event.Name).Inc()

			task_start_delay, err = task_sent_event.GetTaskStartDelayDuration()
			if err != nil {
				Logger.Printf("Unable to parse eta time due to error: %s", err)
			}
		}

		task_id := event.ID()
		stats, ok := TaskStatsMap.Read(task_id)
		if !ok {
			stats = NewTaskStats()
			WaitGroup.Callback.Add(1)
			stats.callBack_timer = time.AfterFunc(Config.StaleTaskCallbackDelayDuration+task_start_delay, func() {
				defer WaitGroup.Callback.Done()

				var is_blacklisted_task bool
				for _, task_name := range Config.BlacklistedTaskNames {
					if task_name == stats.Name {
						is_blacklisted_task = true
					}
				}

				if len(stats.Runtimes) > 0 || stats.Name == "" || is_blacklisted_task {
					// Terminal event received or
					// No sent event received for this task
					// or blacklisted task
					// Hence cannot be considered as a task drop case
					TaskStatsMap.Delete(task_id)
					return
				}

				stats_json, _ := json.Marshal(stats)
				Logger.Printf("Task identified as stale, TaskId: %s, Stats: %s", task_id, stats_json)

				if stats.Name != "" {
					NewCounterVec(prometheus.CounterOpts{
						Name: "celery_task_drop_total",
						Help: "total number of celery task drops",
					}, []string{"task_name", "last_event"}).WithLabelValues(stats.Name, string(stats.GetLatestEvent())).Inc()
				}

				StaleTaskChannel <- &StaleTask{
					TaskId: task_id,
					Stats:  stats,
				}
			})
			TaskStatsMap.Write(task_id, stats)
		} else if event.Type() == TaskEventTypeSent {
			/*
				Resets the callback timer if:
				- task-sent event is received out of order
				- task-sent event is received in case of task retry
			*/
			stats.callBack_timer.Reset(Config.StaleTaskCallbackDelayDuration + task_start_delay)
		}

		event.Process(stats)
		if stats.IsTaskLifecycleComplete() {
			if timer_stopped := stats.callBack_timer.Stop(); timer_stopped {
				WaitGroup.Callback.Done()
			}
			TaskStatsMap.Delete(task_id)
			Logger.Printf("Removed Task (ID: %s) from memory since lifecycle completion reached", task_id)
		}
	}

	Logger.Println("PubSub channel closed")
}

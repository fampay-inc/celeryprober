package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"
)

func consumeStaleTaskChannel() {
	defer WaitGroup.StaleTaskChannelConsumer.Done()
	Logger.Println("Stale task channel listener ready")

	for stale_task := range StaleTaskChannel {
		ctx, cancel := context.WithTimeout(context.Background(), Config.StaleTaskCallbackContextTimeout)

		stats_json, _ := json.Marshal(stale_task.Stats)
		_, err := RedisClient.HSet(ctx, Config.StaleTaskSetKey, stale_task.TaskId.String(), stats_json).Result()
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

		task_id := event.ID()
		stats, ok := TaskStatsMap.Read(task_id)
		if !ok {
			stats = NewTaskStats()
			WaitGroup.Callback.Add(1)
			stats.callBack_timer = time.AfterFunc(Config.StaleTaskCallbackDelayDuration, func() {
				defer WaitGroup.Callback.Done()

				stats_json, _ := json.Marshal(stats)
				Logger.Printf("Task identified as stale, TaskId: %s, Stats: %s", task_id, stats_json)

				StaleTaskChannel <- &StaleTask{
					TaskId: task_id,
					Stats:  stats,
				}
			})
			TaskStatsMap.Write(task_id, stats)
		}

		if event.IsTerminal() {
			if timer_stopped := stats.callBack_timer.Stop(); timer_stopped {
				WaitGroup.Callback.Done()
			}
			TaskStatsMap.Delete(task_id)
		} else {
			event.Process(stats)
		}
	}

	Logger.Println("PubSub channel closed")
}

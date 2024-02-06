package main

import (
	"context"
	"encoding/json"
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

func (report *DailyReport) SentToSlack() {
	report_json, err := json.Marshal(report)
	if err != nil {
		Logger.Fatalln("Unable to marshal DailyReport struct to JSON")
	}

	Logger.Println("Report JSON:", string(report_json))

	status_code, body, err := SendFileToSlackChannel("Task drop report", "task-drop-report.json", report_json)

	if err != nil {
		Logger.Println("Unable to upload file to slack due to error:", err)
	} else {
		Logger.Printf("Slack File Upload API Response Status Code: %d, Response Body: %s", status_code, body)
	}
}

func cron() {
	ctx := context.Background()

	initializeRedis()
	result, err := RedisClient.HGetAll(ctx, Config.StaleTaskSetKey).Result()
	if err != nil {
		Logger.Fatalln("Failed to fetch stale tasks from Redis due to error:", err)
	}

	report := NewDailyReport()
	task_id_list := []string{}

	for task_id, stats_json := range result {
		stats := &TaskStats{}
		if err := json.Unmarshal([]byte(stats_json), stats); err != nil {
			Logger.Printf("Unable to unmarshal stats object, json: %s", stats_json)
			continue
		}

		report.AddDroppedTask(task_id, stats_json)
		task_id_list = append(task_id_list, task_id)
	}

	report.SentToSlack()

	if len(task_id_list) != 0 {
		deleted_key_count, err := RedisClient.HDel(ctx, Config.StaleTaskSetKey, task_id_list...).Result()
		if err != nil {
			Logger.Fatalln("Failed to delete task objects from Redis due to error:", err)
		}

		Logger.Println("Deletion of tasks from Redis hash succeeded, deleted key count:", deleted_key_count)
	}
}

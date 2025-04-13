package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// EventGenerator generates test Celery events
type EventGenerator struct {
	RedisClient *redis.Client
	DBNumber    string
	TaskName    string
	Queue       string
	Hostname    string
}

// NewEventGenerator creates a new event generator
func NewEventGenerator(redisURL, dbNumber, taskName, queue string) (*EventGenerator, error) {
	// Parse the Redis URL
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	// Create Redis client
	client := redis.NewClient(opts)

	return &EventGenerator{
		RedisClient: client,
		DBNumber:    dbNumber,
		TaskName:    taskName,
		Queue:       queue,
		Hostname:    "celery@test-worker-1",
	}, nil
}

// GenerateTaskEvents generates a complete set of task events
func (g *EventGenerator) GenerateTaskEvents(ctx context.Context, shouldFail bool, incompleteStage string) (string, error) {
	// Generate a unique task ID
	taskID := uuid.New().String()
	fmt.Printf("Generated task ID: %s\n", taskID)

	// Define channel prefix
	channelPrefix := fmt.Sprintf("/%s.celeryev/", g.DBNumber)

	// Base clock value
	clock := int(time.Now().Unix())

	// Task args
	args := "(123456,)"
	if shouldFail {
		args = "(123456, 'fail')"
	}

	// 1. Task Sent Event
	err := g.publishEvent(ctx, channelPrefix+"task.sent", taskID, g.TaskName, args, clock, "task-sent", g.Queue)
	if err != nil {
		return "", fmt.Errorf("failed to publish task.sent event: %w", err)
	}
	time.Sleep(500 * time.Millisecond)



	// Check if we should stop at a specific stage to simulate an incomplete task
	if incompleteStage == "sent" {
		return taskID, nil
	}

	// 2. Task Received Event
	err = g.publishEvent(ctx, channelPrefix+"task.received", taskID, g.TaskName, args, clock+1, "task-received", g.Queue)
	if err != nil {
		return "", fmt.Errorf("failed to publish task.received event: %w", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Check if we should stop at received stage
	if incompleteStage == "received" {
		return taskID, nil
	}

	// 3. Task Started Event
	err = g.publishEvent(ctx, channelPrefix+"task.started", taskID, g.TaskName, args, clock+2, "task-started", g.Queue)
	if err != nil {
		return "", fmt.Errorf("failed to publish task.started event: %w", err)
	}
	time.Sleep(1000 * time.Millisecond)

	// Check if we should stop at started stage
	if incompleteStage == "started" {
		return taskID, nil
	}

	// 4. Task Succeeded/Failed Event
	if shouldFail {
		err = g.publishFailedEvent(ctx, channelPrefix+"task.failed", taskID, clock+3)
		if err != nil {
			return "", fmt.Errorf("failed to publish task.failed event: %w", err)
		}
	} else {
		err = g.publishSucceededEvent(ctx, channelPrefix+"task.succeeded", taskID, clock+3)
		if err != nil {
			return "", fmt.Errorf("failed to publish task.succeeded event: %w", err)
		}
	}

	return taskID, nil
}

// publishEvent publishes a Celery event to Redis
func (g *EventGenerator) publishEvent(ctx context.Context, channel, taskID, taskName, args string, clock int, eventType, queue string) error {
	// Create event body
	body := map[string]interface{}{
		"hostname":  g.Hostname,
		"utcoffset": -5,
		"pid":       1,
		"clock":     clock,
		"uuid":      taskID,
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"type":      eventType,
		"root_id":   taskID,
		"parent_id": nil,
	}

	// Add event-specific fields
	if eventType == "task-sent" || eventType == "task-received" {
		body["name"] = taskName
		body["args"] = args
		body["kwargs"] = "{}"
		body["retries"] = 0
		body["eta"] = nil
		body["expires"] = nil
	}

	if eventType == "task-sent" {
		body["exchange"] = ""
		body["routing_key"] = queue
	}

	// Convert body to JSON
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal event body: %w", err)
	}

	// Encode body as base64
	bodyEncoded := base64.StdEncoding.EncodeToString(bodyJSON)

	// Create the full message
	routingKey := strings.TrimPrefix(channel, fmt.Sprintf("/%s.celeryev/", g.DBNumber))
	message := map[string]interface{}{
		"body":             bodyEncoded,
		"content-encoding": "utf-8",
		"content-type":     "application/json",
		"headers": map[string]string{
			"hostname": g.Hostname,
		},
		"properties": map[string]interface{}{
			"delivery_mode": 1,
			"delivery_info": map[string]string{
				"exchange":    "celeryev",
				"routing_key": routingKey,
			},
			"priority":      0,
			"body_encoding": "base64",
			"delivery_tag":  uuid.New().String(),
		},
	}

	// Convert message to JSON
	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Publish to Redis
	err = g.RedisClient.Publish(ctx, channel, messageJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	fmt.Printf("Published %s event to %s\n", eventType, channel)
	return nil
}

// publishSucceededEvent publishes a task succeeded event
func (g *EventGenerator) publishSucceededEvent(ctx context.Context, channel, taskID string, clock int) error {
	// Create event body
	body := map[string]interface{}{
		"hostname":  g.Hostname,
		"utcoffset": -5,
		"pid":       1,
		"clock":     clock,
		"uuid":      taskID,
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"type":      "task-succeeded",
		"root_id":   taskID,
		"parent_id": nil,
		"result":    "success",
		"runtime":   0.8543,
	}

	// Convert body to JSON
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal event body: %w", err)
	}

	// Encode body as base64
	bodyEncoded := base64.StdEncoding.EncodeToString(bodyJSON)

	// Create the full message
	routingKey := strings.TrimPrefix(channel, fmt.Sprintf("/%s.celeryev/", g.DBNumber))
	message := map[string]interface{}{
		"body":             bodyEncoded,
		"content-encoding": "utf-8",
		"content-type":     "application/json",
		"headers": map[string]string{
			"hostname": g.Hostname,
		},
		"properties": map[string]interface{}{
			"delivery_mode": 1,
			"delivery_info": map[string]string{
				"exchange":    "celeryev",
				"routing_key": routingKey,
			},
			"priority":      0,
			"body_encoding": "base64",
			"delivery_tag":  uuid.New().String(),
		},
	}

	// Convert message to JSON
	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Publish to Redis
	err = g.RedisClient.Publish(ctx, channel, messageJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	fmt.Printf("Published task-succeeded event to %s\n", channel)
	return nil
}

// publishFailedEvent publishes a task failed event
func (g *EventGenerator) publishFailedEvent(ctx context.Context, channel, taskID string, clock int) error {
	// Create event body
	body := map[string]interface{}{
		"hostname":  g.Hostname,
		"utcoffset": -5,
		"pid":       1,
		"clock":     clock,
		"uuid":      taskID,
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"type":      "task-failed",
		"root_id":   taskID,
		"parent_id": nil,
		"exception": "ValueError('Test failure')",
		"traceback": "Traceback (most recent call last):\n  File \"test.py\", line 42, in task\n    raise ValueError('Test failure')\nValueError: Test failure",
	}

	// Convert body to JSON
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal event body: %w", err)
	}

	// Encode body as base64
	bodyEncoded := base64.StdEncoding.EncodeToString(bodyJSON)

	// Create the full message
	routingKey := strings.TrimPrefix(channel, fmt.Sprintf("/%s.celeryev/", g.DBNumber))
	message := map[string]interface{}{
		"body":             bodyEncoded,
		"content-encoding": "utf-8",
		"content-type":     "application/json",
		"headers": map[string]string{
			"hostname": g.Hostname,
		},
		"properties": map[string]interface{}{
			"delivery_mode": 1,
			"delivery_info": map[string]string{
				"exchange":    "celeryev",
				"routing_key": routingKey,
			},
			"priority":      0,
			"body_encoding": "base64",
			"delivery_tag":  uuid.New().String(),
		},
	}

	// Convert message to JSON
	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Publish to Redis
	err = g.RedisClient.Publish(ctx, channel, messageJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	fmt.Printf("Published task-failed event to %s\n", channel)
	return nil
}

// Close closes the Redis connection
func (g *EventGenerator) Close() error {
	return g.RedisClient.Close()
}

// extractDBNumberFromRedisURL extracts the database number from a Redis URL
func extractDBNumberFromRedisURL(redisURL string) string {
	// Default database number
	defaultDB := "0"

	// If URL is empty, return default
	if redisURL == "" {
		return defaultDB
	}

	// Extract the path which contains the DB number
	parts := strings.Split(redisURL, "/")
	if len(parts) < 4 {
		return defaultDB
	}

	dbStr := parts[len(parts)-1]
	// Validate that it's a number
	_, err := strconv.Atoi(dbStr)
	if err != nil {
		return defaultDB
	}

	return dbStr
}

func main() {
	// Parse command line arguments
	redisURL := flag.String("redis-url", "redis://localhost:6379/0", "Redis URL")
	dbNumber := flag.String("db", "", "Redis DB number (overrides the one in URL)")
	taskName := flag.String("task", "test_task", "Task name")
	queue := flag.String("queue", "default", "Queue name")
	count := flag.Int("count", 1, "Number of task lifecycles to generate")
	fail := flag.Bool("fail", false, "Generate failed tasks")
	incompleteStage := flag.String("incomplete", "", "Generate incomplete task lifecycle (stop at 'sent', 'received', or 'started')")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help {
		fmt.Println("Celery Task Event Generator")
		fmt.Println("This tool generates Celery task events for testing the celery-monitor application.")
		fmt.Println("\nUsage:")
		flag.PrintDefaults()
		fmt.Println("\nExamples:")
		fmt.Println("  # Generate a successful task event for payment service (DB 0)")
		fmt.Println("  go run cmd/test_events/main.go --redis-url redis://localhost:6379/0 --task payment_process --queue payment")
		fmt.Println("\n  # Generate 5 failed task events for order service (DB 1)")
		fmt.Println("  go run cmd/test_events/main.go --redis-url redis://localhost:6379/1 --db 1 --task order_process --queue order --count 5 --fail")
		fmt.Println("\n  # Generate events for notification service (DB 2)")
		fmt.Println("  go run cmd/test_events/main.go --redis-url redis://localhost:6379/2 --db 2 --task notification_send --queue notification")
		fmt.Println("\n  # Generate incomplete task to test stale task detection (stop at 'sent')")
		fmt.Println("  go run cmd/test_events/main.go --redis-url redis://localhost:6379/0 --task stale_task --incomplete sent")
		os.Exit(0)
	}

	// Extract DB number from Redis URL if not explicitly provided
	db := *dbNumber
	if db == "" {
		db = extractDBNumberFromRedisURL(*redisURL)
	}

	// Create event generator
	generator, err := NewEventGenerator(*redisURL, db, *taskName, *queue)
	if err != nil {
		log.Fatalf("Failed to create event generator: %v", err)
	}
	defer generator.Close()

	// Create context
	ctx := context.Background()

	// Prepare descriptive message about what we're generating
	taskType := map[bool]string{true: "failed", false: "successful"}[*fail]
	if *incompleteStage != "" {
		taskType = "incomplete (" + *incompleteStage + ")"
	}
	fmt.Printf("Generating %d %s task events...\n", *count, taskType)

	// Generate events
	for i := 0; i < *count; i++ {
		taskID, err := generator.GenerateTaskEvents(ctx, *fail, *incompleteStage)
		if err != nil {
			log.Fatalf("Failed to generate task events: %v", err)
		}
		fmt.Printf("Completed task lifecycle %d/%d with ID: %s\n", i+1, *count, taskID)
		if i < *count-1 {
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Println("All test events published successfully!")
}

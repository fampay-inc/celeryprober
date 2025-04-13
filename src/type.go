package main

import (
	"time"

	"github.com/google/uuid"
)

// TaskStats tracks the lifecycle of a Celery task through various events
type TaskStats struct {
	// callBackTimer is used for scheduling cleanup of stale tasks
	callBackTimer            *time.Timer // Keeping original name for compatibility with other code
	Name                     string      `json:"name"`
	Args                     string      `json:"args"`
	LatestEventTimestamp     float64     `json:"latest_event_timestamp"`
	SentTimestamps           []float64   `json:"sent_timestamps"`
	ReceivedTimestamps       []float64   `json:"received_timestamps"`
	StartedTimestamps        []float64   `json:"started_timestamps"`
	SucceededTimestamps      []float64   `json:"succeeded_timestamps"`
	FailedTimestamps         []float64   `json:"failed_timestamps"`
	Runtimes                 []float64   `json:"runtimes"`
	SentRetries              uint8       `json:"sent_retries"`
	ReceivedRetries          uint8       `json:"received_retries"`
	EventsReceivedInSequence bool        `json:"events_received_in_sequence"`
	// ProbeName associates this task with a specific probe
	ProbeName string `json:"probe_name,omitempty"`
}

// NewTaskStats creates and initializes a new TaskStats instance
func NewTaskStats() *TaskStats {
	return &TaskStats{
		SentTimestamps:           []float64{},
		ReceivedTimestamps:       []float64{},
		StartedTimestamps:        []float64{},
		SucceededTimestamps:      []float64{},
		FailedTimestamps:         []float64{},
		Runtimes:                 []float64{},
		EventsReceivedInSequence: true,
	}
}

// IsTaskLifecycleComplete determines if a task has completed its lifecycle
// by analyzing the pattern of events received
func (stats *TaskStats) IsTaskLifecycleComplete() bool {
	succeededEventLen := len(stats.SucceededTimestamps)
	failedEventLen := len(stats.FailedTimestamps)

	// No terminal events yet
	if succeededEventLen == 0 && failedEventLen == 0 {
		return false
	}

	sentEventLength := len(stats.SentTimestamps)
	receivedEventLength := len(stats.ReceivedTimestamps)
	startedEventLength := len(stats.StartedTimestamps)

	retriesSum := stats.SentRetries + stats.ReceivedRetries

	if succeededEventLen > 0 {
		// Normal success case - all events received in sequence
		isSuccessLifecycleComplete :=
			sentEventLength == receivedEventLength &&
				receivedEventLength == startedEventLength &&
				startedEventLength == succeededEventLen
		if isSuccessLifecycleComplete {
			return true
		}

		// Success with retries
		isRetrySuccessLifecycleComplete :=
			retriesSum != 0 &&
				retriesSum%2 == 0 &&
				sentEventLength == receivedEventLength &&
				receivedEventLength == startedEventLength &&
				succeededEventLen == 1
		if isRetrySuccessLifecycleComplete {
			return true
		}
	} else if failedEventLen > 0 {
		// Normal failure case - all events received in sequence
		isFailedLifecycleComplete :=
			sentEventLength == receivedEventLength &&
				receivedEventLength == startedEventLength &&
				startedEventLength == failedEventLen
		if isFailedLifecycleComplete {
			return true
		}

		// Failure with retries
		isRetryFailedLifecycleComplete :=
			retriesSum != 0 &&
				retriesSum%2 == 0 &&
				sentEventLength == receivedEventLength &&
				receivedEventLength == startedEventLength &&
				failedEventLen == 1
		if isRetryFailedLifecycleComplete {
			return true
		}
	}

	// Handle late acknowledgment case
	isPossibleLateAckCaseLifecycleComplete :=
		receivedEventLength > 1 &&
			receivedEventLength == startedEventLength &&
			receivedEventLength > sentEventLength &&
			sentEventLength >= 1
	if isPossibleLateAckCaseLifecycleComplete {
		return true
	}

	return false
}

// GetLatestEvent determines the type of the most recent event for this task
func (stats *TaskStats) GetLatestEvent() TaskEventType {
	// If we have runtimes, it means the task has either succeeded or failed
	if len(stats.Runtimes) > 0 {
		// Check if the latest timestamp matches a success event
		for _, t := range stats.SucceededTimestamps {
			if t == stats.LatestEventTimestamp {
				return TaskEventTypeSucceeded
			}
		}
		// If not a success event but we have runtimes, it must be a failure
		return TaskEventTypeFailed
	}

	// Check if the latest timestamp matches a started event
	for _, t := range stats.StartedTimestamps {
		if t == stats.LatestEventTimestamp {
			return TaskEventTypeStarted
		}
	}

	// Check if the latest timestamp matches a received event
	for _, t := range stats.ReceivedTimestamps {
		if t == stats.LatestEventTimestamp {
			return TaskEventTypeReceived
		}
	}

	// If none of the above, it must be a sent event
	return TaskEventTypeSent
}

// taskStatsMap provides a map of task IDs to their stats
type taskStatsMap map[uuid.UUID]*TaskStats

// Read retrieves a TaskStats from the map
// Note: Caller must handle synchronization with the mutex in the Probe struct
func (m taskStatsMap) Read(key uuid.UUID) (value *TaskStats, ok bool) {
	// The mutex is now part of the Probe struct
	// and is handled by the caller
	value, ok = m[key]
	return
}

// Write adds or updates a TaskStats in the map
// Note: Caller must handle synchronization with the mutex in the Probe struct
func (m taskStatsMap) Write(key uuid.UUID, value *TaskStats) {
	// The mutex is now part of the Probe struct
	// and is handled by the caller
	m[key] = value
}

// Delete removes a TaskStats from the map
// Note: Caller must handle synchronization with the mutex in the Probe struct
func (m taskStatsMap) Delete(key uuid.UUID) {
	// The mutex is now part of the Probe struct
	// and is handled by the caller
	delete(m, key)
}

// StaleTask represents a task that has been identified as stale
// and needs to be processed for cleanup
type StaleTask struct {
	TaskId uuid.UUID
	Stats  *TaskStats
}

// RawEvent represents the raw event data received from the message broker
type RawEvent struct {
	Body            string            `json:"body"`
	ContentEncoding string            `json:"content-encoding"`
	ContentType     string            `json:"content-type"`
	Headers         map[string]string `json:"headers"`
	Properties      map[string]any    `json:"properties"`
}

// TaskEventType defines the possible types of Celery task events
type TaskEventType string

// Event type constants for Celery task lifecycle
const (
	TaskEventTypeSent      TaskEventType = "task-sent"
	TaskEventTypeReceived  TaskEventType = "task-received"
	TaskEventTypeStarted   TaskEventType = "task-started"
	TaskEventTypeSucceeded TaskEventType = "task-succeeded"
	TaskEventTypeFailed    TaskEventType = "task-failed"
)

// TaskEvent represents a Celery task event
// This interface allows for polymorphic handling of different event types
type TaskEvent interface {
	// ID returns the unique identifier for the task
	ID() uuid.UUID

	// Process updates the TaskStats with information from this event
	Process(stats *TaskStats)

	// Type returns the type of this event
	Type() TaskEventType

	// IsTerminal indicates if this event represents the end of a task's lifecycle
	IsTerminal() bool
}


// TaskSent represents a task-sent event from Celery
type TaskSent struct {
	TaskID    uuid.UUID `json:"uuid"` // Renamed for Go conventions but preserving JSON field name
	Timestamp float64   `json:"timestamp"`
	Name      string    `json:"name"`
	Args      string    `json:"args"`
	Retries   uint8     `json:"retries"`
	Queue     string    `json:"queue"`
	ETA       string    `json:"eta"`
}

func (e *TaskSent) ID() uuid.UUID {
	return e.TaskID
}

func (e *TaskSent) Type() TaskEventType {
	return TaskEventTypeSent
}

// Process updates the TaskStats with information from this event
func (e *TaskSent) Process(stats *TaskStats) {
	if CheckIfEventAlreadyProcessed(e.Timestamp, stats.SentTimestamps) {
		return
	}
	stats.Name = e.Name
	stats.Args = e.Args
	stats.SentTimestamps = append(stats.SentTimestamps, e.Timestamp)
	if e.Retries > stats.SentRetries {
		stats.SentRetries = e.Retries
	}
	if e.Timestamp > stats.LatestEventTimestamp {
		stats.LatestEventTimestamp = e.Timestamp
	}
	stats.EventsReceivedInSequence = true

	
}

func (e *TaskSent) IsTerminal() bool {
	return false
}

// GetTaskStartDelayDuration calculates the delay until the task should start
// based on the ETA field
func (e *TaskSent) GetTaskStartDelayDuration() (time.Duration, error) {
	if e.ETA == "" {
		return 0, nil
	}

	taskStartTime, err := time.Parse(time.RFC3339Nano, e.ETA)
	if err != nil {
		return 0, err
	}

	now := time.Now()
	taskStartDelayInSec := taskStartTime.Unix() - now.Unix()
	if taskStartDelayInSec <= 0 {
		return 0, nil
	}

	return time.Duration(taskStartDelayInSec) * time.Second, nil
}


// TaskReceived represents a task-received event from Celery
type TaskReceived struct {
	TaskID    uuid.UUID `json:"uuid"` // Renamed for Go conventions but preserving JSON field name
	Timestamp float64   `json:"timestamp"`
	Retries   uint8     `json:"retries"`
}

func (e *TaskReceived) ID() uuid.UUID {
	return e.TaskID
}

func (e *TaskReceived) Type() TaskEventType {
	return TaskEventTypeReceived
}

// Process updates the TaskStats with information from this event
func (e *TaskReceived) Process(stats *TaskStats) {
	if CheckIfEventAlreadyProcessed(e.Timestamp, stats.ReceivedTimestamps) {
		return
	}
	stats.ReceivedTimestamps = append(stats.ReceivedTimestamps, e.Timestamp)
	if e.Retries > stats.ReceivedRetries {
		stats.ReceivedRetries = e.Retries
	}
	if e.Timestamp > stats.LatestEventTimestamp {
		stats.LatestEventTimestamp = e.Timestamp
	}
	if len(stats.ReceivedTimestamps) > len(stats.SentTimestamps) {
		stats.EventsReceivedInSequence = false
	}

	
}

func (e *TaskReceived) IsTerminal() bool {
	return false
}


// TaskStarted represents a task-started event from Celery
type TaskStarted struct {
	TaskID    uuid.UUID `json:"uuid"` // Renamed for Go conventions but preserving JSON field name
	Timestamp float64   `json:"timestamp"`
}

func (e *TaskStarted) ID() uuid.UUID {
	return e.TaskID
}

func (e *TaskStarted) Type() TaskEventType {
	return TaskEventTypeStarted
}

// Process updates the TaskStats with information from this event
func (e *TaskStarted) Process(stats *TaskStats) {
	if CheckIfEventAlreadyProcessed(e.Timestamp, stats.StartedTimestamps) {
		return
	}
	stats.StartedTimestamps = append(stats.StartedTimestamps, e.Timestamp)
	if len(stats.StartedTimestamps) > len(stats.ReceivedTimestamps) {
		stats.EventsReceivedInSequence = false
	}
	if e.Timestamp > stats.LatestEventTimestamp {
		stats.LatestEventTimestamp = e.Timestamp
	}

	
}

func (e *TaskStarted) IsTerminal() bool {
	return false
}


// TaskSucceeded represents a task-succeeded event from Celery
type TaskSucceeded struct {
	TaskID    uuid.UUID `json:"uuid"` // Renamed for Go conventions but preserving JSON field name
	Timestamp float64   `json:"timestamp"`
	Runtime   float64   `json:"runtime"`
}

func (e *TaskSucceeded) ID() uuid.UUID {
	return e.TaskID
}

func (e *TaskSucceeded) Type() TaskEventType {
	return TaskEventTypeSucceeded
}

// Process updates the TaskStats with information from this event
func (e *TaskSucceeded) Process(stats *TaskStats) {
	if CheckIfEventAlreadyProcessed(e.Timestamp, stats.SucceededTimestamps) {
		return
	}
	stats.SucceededTimestamps = append(stats.SucceededTimestamps, e.Timestamp)
	stats.Runtimes = append(stats.Runtimes, e.Runtime)
	if len(stats.SucceededTimestamps) > len(stats.StartedTimestamps) {
		stats.EventsReceivedInSequence = false
	}
	if e.Timestamp > stats.LatestEventTimestamp {
		stats.LatestEventTimestamp = e.Timestamp
	}

	
}

func (e *TaskSucceeded) IsTerminal() bool {
	return true
}


// TaskFailed represents a task-failed event from Celery
type TaskFailed struct {
	TaskID    uuid.UUID `json:"uuid"` // Renamed for Go conventions but preserving JSON field name
	Timestamp float64   `json:"timestamp"`
	Runtime   float64   `json:"runtime"`
}

func (e *TaskFailed) ID() uuid.UUID {
	return e.TaskID
}

func (e *TaskFailed) Type() TaskEventType {
	return TaskEventTypeFailed
}

// Process updates the TaskStats with information from this event
func (e *TaskFailed) Process(stats *TaskStats) {
	if CheckIfEventAlreadyProcessed(e.Timestamp, stats.FailedTimestamps) {
		return
	}
	stats.FailedTimestamps = append(stats.FailedTimestamps, e.Timestamp)
	stats.Runtimes = append(stats.Runtimes, e.Runtime)
	if len(stats.FailedTimestamps) > len(stats.StartedTimestamps) {
		stats.EventsReceivedInSequence = false
	}
	if e.Timestamp > stats.LatestEventTimestamp {
		stats.LatestEventTimestamp = e.Timestamp
	}

	
}

func (e *TaskFailed) IsTerminal() bool {
	return true
}

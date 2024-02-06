package main

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type TaskStats struct {
	callBack_timer           *time.Timer
	Name                     string    `json:"name"`
	Args                     string    `json:"args"`
	LatestEventTimestamp     float64   `json:"latest_event_timestamp"`
	SentTimestamps           []float64 `json:"sent_timestamps"`
	ReceivedTimestamps       []float64 `json:"received_timestamps"`
	StartedTimestamps        []float64 `json:"started_timestamps"`
	SucceededTimestamps      []float64 `json:"succeeded_timestamps"`
	FailedTimestamps         []float64 `json:"failed_timestamps"`
	Runtimes                 []float64 `json:"runtimes"`
	SentRetries              uint8     `json:"sent_retries"`
	ReceivedRetries          uint8     `json:"received_retries"`
	EventsReceivedInSequence bool      `json:"events_received_in_sequence"`
}

func NewTaskStats() *TaskStats {
	return &TaskStats{
		SentTimestamps:      []float64{},
		ReceivedTimestamps:  []float64{},
		StartedTimestamps:   []float64{},
		SucceededTimestamps: []float64{},
		FailedTimestamps:    []float64{},
		Runtimes:            []float64{},
	}
}

func (stats *TaskStats) IsTaskLifecycleComplete() bool {
	succeeded_event_len := len(stats.SucceededTimestamps)
	failed_event_len := len(stats.FailedTimestamps)

	if succeeded_event_len == 0 && failed_event_len == 0 {
		return false
	}

	sent_event_length := len(stats.SentTimestamps)
	received_event_length := len(stats.ReceivedTimestamps)
	started_event_length := len(stats.StartedTimestamps)

	retries_sum := stats.SentRetries + stats.ReceivedRetries

	if succeeded_event_len > 0 {
		is_success_lifecycle_complete :=
			sent_event_length == received_event_length &&
				received_event_length == started_event_length &&
				started_event_length == succeeded_event_len
		if is_success_lifecycle_complete {
			return true
		}

		is_retry_success_lifecycle_complete :=
			retries_sum != 0 &&
				retries_sum%2 == 0 &&
				sent_event_length == received_event_length &&
				received_event_length == started_event_length &&
				succeeded_event_len == 1
		if is_retry_success_lifecycle_complete {
			return true
		}
	} else if failed_event_len > 0 {
		is_failed_lifecycle_complete :=
			sent_event_length == received_event_length &&
				received_event_length == started_event_length &&
				started_event_length == failed_event_len
		if is_failed_lifecycle_complete {
			return true
		}

		is_retry_failed_lifecycle_complete :=
			retries_sum != 0 &&
				retries_sum%2 == 0 &&
				sent_event_length == received_event_length &&
				received_event_length == started_event_length &&
				failed_event_len == 1
		if is_retry_failed_lifecycle_complete {
			return true
		}
	}

	is_possible_late_ack_case_lifecycle_complete :=
		received_event_length > 1 &&
			received_event_length == started_event_length &&
			received_event_length > sent_event_length &&
			sent_event_length >= 1
	if is_possible_late_ack_case_lifecycle_complete {
		return true
	}

	return false
}

func (stats *TaskStats) GetLatestEvent() TaskEventType {
	if len(stats.Runtimes) > 0 {
		for _, t := range stats.SucceededTimestamps {
			if t == stats.LatestEventTimestamp {
				return TaskEventTypeSucceeded
			}
		}
		return TaskEventTypeFailed
	}

	for _, t := range stats.StartedTimestamps {
		if t == stats.LatestEventTimestamp {
			return TaskEventTypeStarted
		}
	}

	for _, t := range stats.ReceivedTimestamps {
		if t == stats.LatestEventTimestamp {
			return TaskEventTypeReceived
		}
	}

	return TaskEventTypeSent
}

type waitGroup struct {
	PubSubChannelConsumer    sync.WaitGroup
	StaleTaskChannelConsumer sync.WaitGroup
	Callback                 sync.WaitGroup
}

type taskStatsMap map[uuid.UUID]*TaskStats

func (m taskStatsMap) Read(key uuid.UUID) (value *TaskStats, ok bool) {
	TaskStatsMapMutex.RLock()
	defer TaskStatsMapMutex.RUnlock()

	value, ok = TaskStatsMap[key]
	return
}

func (m taskStatsMap) Write(key uuid.UUID, value *TaskStats) {
	TaskStatsMapMutex.Lock()
	defer TaskStatsMapMutex.Unlock()

	TaskStatsMap[key] = value
}

func (m taskStatsMap) Delete(key uuid.UUID) {
	TaskStatsMapMutex.Lock()
	defer TaskStatsMapMutex.Unlock()

	delete(TaskStatsMap, key)
}

type StaleTask struct {
	TaskId uuid.UUID
	Stats  *TaskStats
}

type RawEvent struct {
	Body            string            `json:"body"`
	ContentEncoding string            `json:"content-encoding`
	ContentType     string            `json:"content-type"`
	Headers         map[string]string `json:"headers"`
	Properties      map[string]any    `json:"properties"`
}

type TaskEventType string

const (
	TaskEventTypeSent      TaskEventType = "task-sent"
	TaskEventTypeReceived  TaskEventType = "task-received"
	TaskEventTypeStarted   TaskEventType = "task-started"
	TaskEventTypeSucceeded TaskEventType = "task-succeeded"
	TaskEventTypeFailed    TaskEventType = "task-failed"
)

type TaskEvent interface {
	ID() uuid.UUID
	Process(stats *TaskStats)
	Type() TaskEventType
	IsTerminal() bool
}

// +-------------------- Task Sent Begins --------------------+

type TaskSent struct {
	TaskId    uuid.UUID `json:"uuid"`
	Timestamp float64   `json:"timestamp"`
	Name      string    `json:"name"`
	Args      string    `json:"args"`
	Retries   uint8     `json:"retries"`
	Queue     string    `json:"queue"`
	ETA       string    `json:"eta"`
}

func (e *TaskSent) ID() uuid.UUID {
	return e.TaskId
}

func (e *TaskSent) Type() TaskEventType {
	return TaskEventTypeSent
}

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

func (e *TaskSent) GetTaskStartDelayDuration() (duration time.Duration, err error) {
	if e.ETA == "" {
		return
	}

	task_start_time, err := time.Parse(time.RFC3339Nano, e.ETA)
	if err != nil {
		return
	}

	task_start_delay_in_sec := task_start_time.Unix() - time.Now().Unix()
	if task_start_delay_in_sec > 0 {
		duration = time.Duration(task_start_delay_in_sec) * time.Second
	}

	return
}

// +-------------------- Task Received Begins --------------------+

type TaskReceived struct {
	TaskId    uuid.UUID `json:"uuid"`
	Timestamp float64   `json:"timestamp"`
	Retries   uint8     `json:"retries"`
}

func (e *TaskReceived) ID() uuid.UUID {
	return e.TaskId
}

func (e *TaskReceived) Type() TaskEventType {
	return TaskEventTypeReceived
}

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

// +-------------------- Task Started Begins --------------------+

type TaskStarted struct {
	TaskId    uuid.UUID `json:"uuid"`
	Timestamp float64   `json:"timestamp"`
}

func (e *TaskStarted) ID() uuid.UUID {
	return e.TaskId
}

func (e *TaskStarted) Type() TaskEventType {
	return TaskEventTypeStarted
}

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

// +-------------------- Task Succeeded Begins --------------------+

type TaskSucceeded struct {
	TaskId    uuid.UUID `json:"uuid"`
	Timestamp float64   `json:"timestamp"`
	Runtime   float64   `json:"runtime"`
}

func (e *TaskSucceeded) ID() uuid.UUID {
	return e.TaskId
}

func (e *TaskSucceeded) Type() TaskEventType {
	return TaskEventTypeSucceeded
}

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

// +-------------------- Task Failed Begins --------------------+

type TaskFailed struct {
	TaskId    uuid.UUID `json:"uuid"`
	Timestamp float64   `json:"timestamp"`
	Runtime   float64   `json:"runtime"`
}

func (e *TaskFailed) ID() uuid.UUID {
	return e.TaskId
}

func (e *TaskFailed) Type() TaskEventType {
	return TaskEventTypeFailed
}

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

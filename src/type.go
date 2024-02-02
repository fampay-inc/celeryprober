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
	stats.EventsReceivedInSequence = true
}

func (e *TaskSent) IsTerminal() bool {
	return false
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
	stats.StartedTimestamps = append(stats.StartedTimestamps, e.Timestamp)
	stats.Runtimes = append(stats.Runtimes, e.Runtime)
	if len(stats.FailedTimestamps) > len(stats.StartedTimestamps) {
		stats.EventsReceivedInSequence = false
	}
}

func (e *TaskFailed) IsTerminal() bool {
	return true
}

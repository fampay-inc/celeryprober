package main

import (
	"time"

	"github.com/caarlos0/env/v10"
)

type Configuration struct {
	StaleTaskCallbackContextTimeout      time.Duration
	StaleTaskCallbackDelayDuration       time.Duration
	StaleTaskCallbackContextTimeoutInSec int      `env:"STALE_TASK_CALLBACK_CONTEXT_TIMEOUT_IN_SEC" envDefault:"5"`
	StaleTaskCallbackDelayDurationInMin  int      `env:"STALE_TASK_CALLBACK_DELAY_DURATION_IN_MIN" envDefault:"15"`
	RedisURL                             string   `env:"REDIS_URL" envDefault:"redis://localhost:6379/6"`
	StaleTaskSetKey                      string   `env:"STALE_TASK_SET_KEY" envDefault:"stale_tasks"`
	TaskEventChannels                    []string `env:"TASK_EVENT_CHANNELS" envDefault:"/6.celeryev/task.sent,/6.celeryev/task.received,/6.celeryev/task.started,/6.celeryev/task.succeeded,/6.celeryev/task.failed" envSeparator:","`
}

func NewConfig() *Configuration {
	config := &Configuration{}
	if err := env.Parse(config); err != nil {
		Logger.Fatal("Unable to parse Configuration")
	}

	config.StaleTaskCallbackContextTimeout = time.Duration(config.StaleTaskCallbackContextTimeoutInSec) * time.Second
	config.StaleTaskCallbackDelayDuration = time.Duration(config.StaleTaskCallbackDelayDurationInMin) * time.Minute

	return config
}

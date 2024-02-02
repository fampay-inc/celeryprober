package main

import (
	"github.com/caarlos0/env/v10"
)

type Configuration struct {
	RedisURL          string   `env:"REDIS_URL" envDefault:"redis://localhost:6379/6"`
	StaleTaskSetKey   string   `env:"STALE_TASK_SET_KEY" envDefault:"stale_tasks"`
	TaskEventChannels []string `env:"TASK_EVENT_CHANNELS" envDefault:"/6.celeryev/task.sent,/6.celeryev/task.received,/6.celeryev/task.started,/6.celeryev/task.succeeded,/6.celeryev/task.failed" envSeparator:","`
}

func NewConfig() *Configuration {
	config := &Configuration{}
	if err := env.Parse(config); err != nil {
		Logger.Fatal("Unable to parse Configuration")
	}
	return config
}

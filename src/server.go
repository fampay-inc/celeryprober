package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	Config            *Configuration
	Logger            *log.Logger
	WaitGroup         waitGroup
	RedisClient       *redis.Client
	PubSub            *redis.PubSub
	TaskStatsMap      taskStatsMap
	TaskStatsMapMutex sync.RWMutex
	StaleTaskChannel  chan *StaleTask
)

func initializePubSub(ctx context.Context) {
	Logger.Println("Starting PubSub Subcriber...")

	PubSub = RedisClient.Subscribe(ctx, Config.TaskEventChannels...)
	if err := PubSub.Ping(ctx, "ping"); err != nil {
		Logger.Fatalln("Cannot ping PubSub due to error:", err)
	}

	Logger.Println("Subscribed to channels:", Config.TaskEventChannels)
}

func initializeStore() {
	TaskStatsMap = map[uuid.UUID]*TaskStats{}
	TaskStatsMapMutex = sync.RWMutex{}
	StaleTaskChannel = make(chan *StaleTask)
}

func initializeListners() {
	WaitGroup.StaleTaskChannelConsumer.Add(1)
	go consumeStaleTaskChannel()
	WaitGroup.PubSubChannelConsumer.Add(1)
	go consumePubSubChannel()
}

func waitForInterrupt(ctx context.Context) {
	<-ctx.Done()
}

func gracefulShutdown(ctx context.Context) {
	waitForInterrupt(ctx)
	Logger.Println("Waiting for tasks to be finished...")

	PubSub.Close()
	WaitGroup.PubSubChannelConsumer.Wait()

	Logger.Println("Waiting for scheduled callbacks to be executed...")
	WaitGroup.Callback.Wait()

	Logger.Println("Waiting for stale tasks to be pushed to Redis...")
	close(StaleTaskChannel)
	WaitGroup.StaleTaskChannelConsumer.Wait()

	RedisClient.Close()

	Logger.Println("Service stopped gracefully")
}

func initializeMetrics() {
	// Initialize metrics with service name from config
	InitMetrics(Config.ServiceName)
	Logger.Printf("Initialized metrics with service name: %s", Config.ServiceName)
}

func server() {
	Logger.Println("Starting server...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	initializeRedis()
	initializePubSub(ctx)
	initializeStore()
	initializeMetrics()
	initializeListners()

	RunRESTServer()
	RunMetricsServer()

	gracefulShutdown(ctx)
}

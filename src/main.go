package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	Config            *Configuration
	Logger            *log.Logger
	Wg                sync.WaitGroup
	RedisClient       *redis.Client
	PubSub            *redis.PubSub
	TaskStatsMap      taskStatsMap
	TaskStatsMapMutex sync.RWMutex
	StaleTaskChannel  chan *StaleTask
)

func initializeRedis(ctx context.Context) {
	redisClientOptions, err := redis.ParseURL(Config.RedisURL)
	if err != nil {
		Logger.Fatalln("Cannot parse RedisURL:", Config.RedisURL)
	}

	RedisClient = redis.NewClient(redisClientOptions)

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
	Wg.Add(2)
	go handleStaleTasks()
	go consumeEventChannel()
}

func waitForInterrupt(ctx context.Context) {
	<-ctx.Done()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	Config = NewConfig()
	Logger = log.Default()

	initializeRedis(ctx)
	initializeStore()
	initializeListners()

	// Graceful shutdown begins here...
	waitForInterrupt(ctx)
	Logger.Println("Waiting for tasks to be finished...")

	close(StaleTaskChannel)
	PubSub.Close()
	Wg.Wait()

	RedisClient.Close()

	Logger.Println("Service stopped gracefully")
}

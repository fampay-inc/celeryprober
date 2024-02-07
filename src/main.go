package main

import (
	"log"

	"github.com/redis/go-redis/v9"
)

func initializeRedis() {
	redisClientOptions, err := redis.ParseURL(Config.RedisURL)
	if err != nil {
		Logger.Fatalln("Cannot parse RedisURL:", Config.RedisURL)
	}

	redisClientOptions.ClientName = Config.ServiceName
	RedisClient = redis.NewClient(redisClientOptions)
}

func main() {
	Config = NewConfig()
	Logger = log.Default()

	switch Config.Mode {
	case Server:
		server()
	case Cron:
		cron()
	}
}

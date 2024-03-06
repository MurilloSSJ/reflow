package subscriber

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisSubscriber struct {
	client *redis.Client
	ctx    context.Context
}

func NewSubscriber(name string) *RedisSubscriber {
	client := &RedisSubscriber{
		client: redis.NewClient(&redis.Options{
			Addr:     "172.18.0.2:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		}),
		ctx: context.Background(),
	}
	result_ping := client.client.Ping(client.ctx)
	fmt.Println(result_ping)
	return client
}

func (rp *RedisSubscriber) Update(message interface{}) {
	// This is the actual implementation of the Update method
	response := rp.client.Publish(rp.ctx, "tasks", message)
	fmt.Println(response.Val())
}

package publisher

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisPublisher struct {
	client *redis.Client
	ctx    context.Context
}

func NewPublisher(name string) *RedisPublisher {
	client := &RedisPublisher{
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

func (rp *RedisPublisher) Subscribe() *redis.PubSub {
	// This is the actual implementation of the Update method
	return rp.client.PSubscribe(rp.ctx, "tasks")
}
func (rp *RedisPublisher) Receive(pubsub *redis.PubSub) {
	// This is the actual implementation of the Update method
	fmt.Println(pubsub.Receive(rp.ctx))
}

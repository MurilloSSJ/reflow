package main

import (
	"fmt"
	"reflow/scheduler/v2/src/publisher"
	"reflow/scheduler/v2/src/subscriber"
	"time"
)

func main() {
	ticker := time.NewTicker(1 * time.Minute)
	subscriber := subscriber.NewSubscriber("redis")
	publisher := publisher.NewPublisher("redis")
	pubsub := publisher.Subscribe()
	quit := make(chan struct{})
	fmt.Println("Scheduler is running...")
	for {
		fmt.Println("Waiting for updates...")
		select {
		case <-ticker.C:
			fmt.Println("Updating...")
			go subscriber.Update("Hello, World!")
			go publisher.Receive(pubsub)
		case <-quit:
			ticker.Stop()
			return
		}
	}
}

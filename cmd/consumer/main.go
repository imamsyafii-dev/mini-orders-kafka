package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/imamsyafii-dev/mini-orders-kafka/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic         = "orders.v1"
	consumerGroup = "orders-learning-v1"
)

func main() {
	name := flag.String("name", "worker-unnamed", "Nama consumer untuk log")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("create Kafka client: %v", err)
	}
	defer client.Close()

	fmt.Printf(
		"consumer=%s started group=%s topic=%s; press Ctrl+C to stop\n",
		*name,
		consumerGroup,
		topic,
	)

	for {
		fetches := client.PollFetches(ctx)

		if ctx.Err() != nil {
			fmt.Printf("\nconsumer=%s stopped\n", *name)
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				log.Printf("consumer=%s consume error: %v", *name, fetchErr.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			var orderCreated event.OrderCreated
			if err := json.Unmarshal(record.Value, &orderCreated); err != nil {
				log.Printf("consumer=%s decode event: %v", *name, err)
				return
			}

			fmt.Printf(
				"consumer=%s order_id=%s sequence=%d partition=%d offset=%d\n",
				*name,
				orderCreated.Payload.OrderID,
				orderCreated.Payload.Sequence,
				record.Partition,
				record.Offset,
			)
		})
	}
}

func brokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return []string{"localhost:9092"}
	}

	return strings.Split(raw, ",")
}

package main

import (
	"context"
	"encoding/json"
	"errors"
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

const topic = "orders.v1"

func main() {
	name := flag.String("name", "worker-unnamed", "Nama consumer untuk log")
	group := flag.String("group", "orders-learning-v1", "Kafka consumer group")
	offsetReset := flag.String("offset-reset", "earliest", "Offset awal untuk group baru: earliest atau latest")
	crashBeforeCommit := flag.Bool(
		"crash-before-commit",
		false,
		"Simulasikan crash setelah processing dan sebelum commit",
	)
	flag.Parse()

	resetOffset, err := parseOffsetReset(*offsetReset)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(resetOffset),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("create Kafka client: %v", err)
	}
	defer client.Close()

	fmt.Printf(
		"consumer=%s started group=%s topic=%s auto_commit=false; press Ctrl+C to stop\n",
		*name,
		*group,
		topic,
	)

	for {
		fetches := client.PollRecords(ctx, 1)

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

		var processingErr error

		fetches.EachRecord(func(record *kgo.Record) {
			if processingErr != nil {
				return
			}

			processingErr = processRecord(
				ctx,
				client,
				record,
				*name,
				*crashBeforeCommit,
			)
		})

		if processingErr != nil {
			log.Fatal(processingErr)
		}
	}
}

func processRecord(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	name string,
	crashBeforeCommit bool,
) error {
	var orderCreated event.OrderCreated
	if err := json.Unmarshal(record.Value, &orderCreated); err != nil {
		return fmt.Errorf("decode event at partition=%d offset=%d: %w",
			record.Partition,
			record.Offset,
			err,
		)
	}

	fmt.Printf(
		"consumer=%s received event_id=%s order_id=%s sequence=%d partition=%d offset=%d\n",
		name,
		orderCreated.EventID,
		orderCreated.Payload.OrderID,
		orderCreated.Payload.Sequence,
		record.Partition,
		record.Offset,
	)

	fmt.Printf(
		"consumer=%s processed event_id=%s partition=%d offset=%d\n",
		name,
		orderCreated.EventID,
		record.Partition,
		record.Offset,
	)

	if crashBeforeCommit {
		fmt.Fprintf(
			os.Stderr,
			"consumer=%s simulating crash before commit event_id=%s\n",
			name,
			orderCreated.EventID,
		)
		os.Exit(1)
	}

	if err := client.CommitRecords(ctx, record); err != nil {
		return fmt.Errorf(
			"commit event_id=%s partition=%d offset=%d: %w",
			orderCreated.EventID,
			record.Partition,
			record.Offset,
			err,
		)
	}

	fmt.Printf(
		"consumer=%s committed event_id=%s next_offset=%d\n",
		name,
		orderCreated.EventID,
		record.Offset+1,
	)

	return nil
}

func parseOffsetReset(value string) (kgo.Offset, error) {
	switch value {
	case "earliest":
		return kgo.NewOffset().AtStart(), nil
	case "latest":
		return kgo.NewOffset().AtEnd(), nil
	default:
		return kgo.NewOffset(), errors.New(
			"offset-reset harus bernilai earliest atau latest",
		)
	}
}

func brokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return []string{"localhost:9092"}
	}

	return strings.Split(raw, ",")
}

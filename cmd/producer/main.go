package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/imamsyafii-dev/mini-orders-kafka/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

const topic = "orders.v1"

func main() {
	orderID := flag.String("order-id", "", "ID order")
	customerID := flag.String("customer-id", "", "ID customer")
	amount := flag.Int64("amount", 0, "Total order dalam rupiah")
	count := flag.Int("count", 1, "Jumlah event berurutan untuk order yang sama")
	flag.Parse()

	if *orderID == "" || *customerID == "" || *amount <= 0 || *count <= 0 {
		log.Fatal("order-id, customer-id, amount, dan count harus valid")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
	)
	if err != nil {
		log.Fatalf("create Kafka client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for sequence := 1; sequence <= *count; sequence++ {
		orderCreated := event.OrderCreated{
			EventID:    fmt.Sprintf("evt-%d", time.Now().UnixNano()),
			EventType:  "order.created",
			OccurredAt: time.Now().UTC(),
			Payload: event.OrderCreatedData{
				OrderID:    *orderID,
				CustomerID: *customerID,
				Amount:     *amount,
				Sequence:   sequence,
			},
		}

		value, err := json.Marshal(orderCreated)
		if err != nil {
			log.Fatalf("encode order event: %v", err)
		}

		delivered, err := client.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(*orderID),
			Value: value,
		}).First()
		if err != nil {
			log.Fatalf("publish order event: %v", err)
		}

		fmt.Printf(
			"published order_id=%s sequence=%d partition=%d offset=%d\n",
			orderCreated.Payload.OrderID,
			orderCreated.Payload.Sequence,
			delivered.Partition,
			delivered.Offset,
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

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
	flag.Parse()

	if *orderID == "" || *customerID == "" || *amount <= 0 {
		log.Fatal("order-id, customer-id, dan amount wajib diisi; amount harus lebih dari 0")
	}

	orderCreated := event.OrderCreated{
		EventID:    fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		EventType:  "order.created",
		OccurredAt: time.Now().UTC(),
		Payload: event.OrderCreatedData{
			OrderID:    *orderID,
			CustomerID: *customerID,
			Amount:     *amount,
		},
	}

	value, err := json.Marshal(orderCreated)
	if err != nil {
		log.Fatalf("encode order event: %v", err)
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

	result := client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(*orderID),
		Value: value,
	})

	if err := result.FirstErr(); err != nil {
		log.Fatalf("publish order event: %v", err)
	}

	fmt.Printf(
		"published event_id=%s order_id=%s topic=%s\n",
		orderCreated.EventID,
		orderCreated.Payload.OrderID,
		topic,
	)
}

func brokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return []string{"localhost:9092"}
	}

	return strings.Split(raw, ",")
}

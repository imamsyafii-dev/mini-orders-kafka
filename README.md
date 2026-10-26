# Mini Orders Kafka

Project belajar Apache Kafka dengan Go.

## Arsitektur tahap 1

```text
Producer Go
    |
    | OrderCreated
    v
Kafka topic: orders.v1
    |
    v
Consumer Go
```

## Prasyarat

- Go
- Docker Desktop / Docker Engine dengan Docker Compose

## Menjalankan Kafka

```bash
docker compose up -d --wait
```

## Membuat topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic orders.v1 \
  --partitions 1 \
  --replication-factor 1
```

## Menjalankan consumer

```bash
go run ./cmd/consumer
```

## Mengirim order

Buka terminal lain:

```bash
go run ./cmd/producer \
  --order-id order-001 \
  --customer-id customer-001 \
  --amount 125000
```

## Menghentikan Kafka

```bash
docker compose down
```

## Yang dipelajari pada tahap ini

- Kafka broker dan topic
- Kafka record: key dan value
- Producer
- Consumer group
- Partition dan offset
- Event JSON dasar
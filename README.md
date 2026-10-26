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

## Tahap 2: partition, key, dan consumer group

Topic `orders.v1` memakai tiga partition.

Kafka hanya menjamin urutan dalam partition yang sama. Producer memakai
`order_id` sebagai key, sehingga seluruh event dengan `order_id` yang sama
masuk ke partition yang sama dan mempertahankan urutan sequence.

### Naikkan jumlah partition

Untuk repository yang sudah menjalankan Tahap 1:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders.v1 \
  --partitions 3
```

Verifikasi:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders.v1
```

### Jalankan dua consumer dalam group yang sama

Terminal A:

```bash
go run ./cmd/consumer --name worker-a
```

Terminal B:

```bash
go run ./cmd/consumer --name worker-b
```

Karena topic memiliki tiga partition, dua consumer dalam group yang sama akan
membagi pekerjaan: satu worker biasanya menangani dua partition dan worker
lain menangani satu partition.

### Buktikan ordering untuk satu key

Terminal C:

```bash
go run ./cmd/producer \
  --order-id order-001 \
  --customer-id customer-001 \
  --amount 125000 \
  --count 10
```

Semua output producer harus menampilkan partition yang sama. Consumer harus
menampilkan sequence `1` sampai `10` dalam urutan yang sama.

### Buktikan distribusi beberapa key

```bash
for order_id in order-002 order-003 order-004 order-005 order-006 order-007 order-008 order-009 order-010 order-011 order-012; do
  go run ./cmd/producer \
    --order-id "$order_id" \
    --customer-id customer-001 \
    --amount 125000
done
```

Amati partition pada output producer dan nama worker pada output consumer.
Hash key dapat membuat beberapa order masuk partition yang sama; kirim key
tambahan sampai seluruh partition terlihat.

### Buktikan rebalance

1. Hentikan `worker-b` dengan `Ctrl+C`.
2. Tunggu beberapa detik.
3. Kirim beberapa order dengan key berbeda lagi.
4. Pastikan `worker-a` menerima event dari semua partition.

Tahap ini masih memakai auto-commit. Manual commit, retry, DLQ, dan
idempotency akan dibahas pada tahap berikutnya.

## Tahap 3: manual commit dan replay

Consumer Tahap 3 menonaktifkan auto-commit. Urutan prosesnya adalah:

```text
fetch → decode → processed → commit
```

Jika sebuah record berada di offset `12`, commit sukses berarti Kafka menyimpan
posisi berikutnya sebagai offset `13`.

### Jalur sukses

Terminal A:

```bash
go run ./cmd/consumer \
  --name stage3-worker \
  --group orders-stage3 \
  --offset-reset latest
```

Tunggu sampai consumer aktif, lalu di Terminal B:

```bash
go run ./cmd/producer \
  --order-id stage3-order \
  --customer-id customer-stage3 \
  --amount 175000
```

Consumer harus menampilkan urutan:

```text
received → processed → committed
```

Periksa committed offset:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-stage3
```

### Restart tanpa replay

1. Hentikan consumer dengan `Ctrl+C`.
2. Jalankan consumer lagi dengan group yang sama.
3. Event yang telah di-commit tidak boleh muncul lagi.
4. Hentikan consumer tersebut dengan `Ctrl+C` sebelum melanjutkan ke demo crash.

### Crash sebelum commit

Terminal A:

```bash
go run ./cmd/consumer \
  --name crash-worker \
  --group orders-stage3 \
  --offset-reset latest \
  --crash-before-commit
```

Terminal B:

```bash
go run ./cmd/producer \
  --order-id stage3-order \
  --customer-id customer-stage3 \
  --amount 175000
```

Consumer harus menampilkan `received`, `processed`, lalu `simulating crash before commit`.
Tidak boleh ada log `committed`.

Jalankan kembali consumer tanpa flag crash:

```bash
go run ./cmd/consumer \
  --name restarted-worker \
  --group orders-stage3 \
  --offset-reset latest
```

Event dengan `event_id`, partition, dan offset yang sama harus muncul lagi, lalu
berhasil di-commit.

Ini adalah perilaku **at-least-once**: side effect dapat terjadi dua kali jika
proses crash setelah processing tetapi sebelum offset di-commit. Idempotency,
retry, dan DLQ akan dibahas pada tahap berikutnya.

## Tahap 4: idempotency dengan SQLite

Tahap ini memakai SQLite sebagai inbox/idempotency store.

Dalam satu transaksi database, consumer:

1. Menyimpan `event_id` ke `processed_events`.
2. Menulis simulated business side effect ke `business_effects`.
3. Baru setelah transaksi database sukses, commit offset Kafka.

Jika consumer crash setelah transaksi SQLite sukses tetapi sebelum commit Kafka,
Kafka me-replay event yang sama. SQLite sudah mengenali `event_id`, sehingga
business effect tidak dibuat dua kali.

### Menjalankan demo

Gunakan database dan consumer group baru:

Terminal A:

```bash
go run ./cmd/consumer \
  --name stage4-worker \
  --group orders-stage4 \
  --offset-reset latest \
  --db-path ./data/orders-stage4.db
```

Terminal B:

```bash
go run ./cmd/producer \
  --order-id stage4-order \
  --customer-id customer-stage4 \
  --amount 200000
```

Consumer harus menampilkan:

```text
received → business_effect_applied → offset_committed
```

Jika `sqlite3` tersedia, periksa jumlah side effect:

```bash
sqlite3 ./data/orders-stage4.db \
  'SELECT COUNT(*) FROM business_effects;'
```

Hasilnya `1`.

### Crash setelah database commit

1. Hentikan consumer normal.
2. Jalankan consumer dengan database dan group sama:

```bash
go run ./cmd/consumer \
  --name crash-worker \
  --group orders-stage4 \
  --offset-reset latest \
  --db-path ./data/orders-stage4.db \
  --crash-before-commit
```

3. Kirim satu event baru dengan key yang sama:

```bash
go run ./cmd/producer \
  --order-id stage4-order \
  --customer-id customer-stage4 \
  --amount 210000
```

Consumer harus menampilkan `business_effect_applied`, lalu crash tanpa
`offset_committed`. Jumlah `business_effects` menjadi `2`.

4. Jalankan kembali consumer tanpa flag crash:

```bash
go run ./cmd/consumer \
  --name replay-worker \
  --group orders-stage4 \
  --offset-reset latest \
  --db-path ./data/orders-stage4.db
```

Event yang sama akan direplay, tetapi consumer menampilkan:

```text
duplicate_skipped → offset_committed
```

Jumlah `business_effects` harus tetap `2`, bukan `3`.

### Batasan

- Ini bukan exactly-once global.
- Email, HTTP request, atau database lain tidak otomatis menjadi idempotent.
- `event_id` harus stabil saat Kafka me-replay record.
- SQLite file lokal cocok untuk lab atau single instance; production multi-instance
  membutuhkan database bersama dengan unique constraint yang setara.
- Retention/cleanup `processed_events` belum dibahas.

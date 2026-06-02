# 🚀 Kafka + PySpark Structured Streaming — Transaction Pipeline

Assignment: Real-time transaction validation menggunakan Kafka Producer + PySpark Structured Streaming.

---

## 📁 Struktur Folder

```
.
├── docker-compose.yml
├── producer/
│   ├── producer.py          # Kafka event producer
│   └── requirements.txt
├── streaming/
│   └── spark_streaming_job.py   # PySpark Structured Streaming job
└── README.md
```

---

## ⚙️ Prerequisites (GitHub Codespaces)

Tidak perlu install apapun secara manual. Semua service berjalan via Docker Compose.

---

## 🐳 Setup & Menjalankan

### Step 1 — Start semua service

```bash
docker-compose up -d
```

Tunggu ~30 detik sampai Kafka siap. Cek status:

```bash
docker-compose ps
```

### Step 2 — Buat Kafka topics (jika belum auto-create)

```bash
docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 1 --replication-factor 1

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic transactions_valid \
  --partitions 1 --replication-factor 1

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic transactions_dlq \
  --partitions 1 --replication-factor 1
```

### Step 3 — Jalankan Spark Streaming Job

Buka **terminal pertama**:

```bash
docker-compose logs -f spark
```

Atau jalankan manual jika mau lihat console output langsung:

```bash
docker exec -it spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /app/streaming/spark_streaming_job.py
```

### Step 4 — Jalankan Producer

Buka **terminal kedua**:

```bash
pip install kafka-python
python producer/producer.py
```

---

## 📊 Arsitektur Pipeline

```
producer.py
    │
    ├── INVALID events (3+) ─────────────────────────┐
    ├── LATE events (3+, timestamp > 3 menit lalu) ──┤
    └── VALID events (continuous, 1-2 detik) ─────── ▼
                                              Kafka: transactions
                                                    │
                                         PySpark Structured Streaming
                                                    │
                                    ┌───────────────┴───────────────┐
                                    │          5 Validasi           │
                                    │  1. Mandatory field check     │
                                    │  2. Type validation           │
                                    │  3. Range (amount 1-10jt)     │
                                    │  4. Source validation         │
                                    │  5. Duplicate detection       │
                                    └───────────┬───────────────────┘
                                                │
                        ┌───────────────────────┴───────────────────┐
                        ▼                                           ▼
              is_valid = True                             is_valid = False
         Kafka: transactions_valid                   Kafka: transactions_dlq
```

---

## ✅ Fitur yang Diimplementasikan

| Requirement | Status | Detail |
|---|---|---|
| Event producer kirim tiap 1-2 detik | ✅ | `random.uniform(1, 2)` |
| Min 3 invalid events | ✅ | 6 invalid dikirim (amount negatif, terlalu besar, timestamp invalid, source tidak dikenal, 2x duplicate) |
| Min 3 late events | ✅ | 3 late events (5, 7, 10 menit lalu) |
| Publish ke topic `transactions` | ✅ | |
| Read dari Kafka + deserialize JSON | ✅ | Schema terdefinisi |
| 5 validasi wajib | ✅ | Mandatory field, type, range, source, duplicate |
| Kolom `is_valid` dan `error_reason` | ✅ | |
| Routing valid → `transactions_valid` | ✅ | |
| Routing invalid → `transactions_dlq` | ✅ | |
| Watermark `.withWatermark("event_time", "3 minutes")` | ✅ | |
| Late event > 3 menit → DLQ | ✅ | `LATE_EVENT_EXPIRED` |
| Tumbling window 1 menit | ✅ | Total transaksi per window |
| Output `timestamp` + `running_total` | ✅ | via `foreachBatch` |

---

## 🔍 Kafka UI (Opsional)

Buka browser: `http://localhost:8080`

Bisa monitor topics `transactions`, `transactions_valid`, `transactions_dlq` secara real-time.

---

## 🛑 Stop Semua Service

```bash
docker-compose down
```

---

## 📸 Screenshots

*(Tambahkan screenshot di sini setelah menjalankan)*

- [ ] Screenshot Kafka producer (terminal output)
- [ ] Screenshot Spark streaming console output
- [ ] Screenshot Kafka UI topics valid & DLQ (opsional)

# Kafka Streaming - Transaction Pipeline

Project ini buat simulasi real-time transaction processing pakai Kafka + PySpark Structured Streaming.

## Struktur
producer/producer.py — script buat kirim event ke Kafka
streaming/spark_streaming_job.py — PySpark job buat validasi & proses event
docker-compose.yml — setup Kafka, Zookeeper, Kafka UI
## Cara Jalanin

Start Docker dulu:
```bash
docker-compose up -d
Jalanin Spark job:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming/spark_streaming_job.py
Jalanin producer:
pip install kafka-python
python producer/producer.py
Notes
Producer ngirim event valid, invalid, sama late events
Spark validasi 5 hal: mandatory field, type, range amount, source, duplicate
Event valid masuk ke topic transactions_valid, yang gagal ke transactions_dlq
Pakai watermark 3 menit + tumbling window 1 menit buat monitoring
# Streaming README

This project demonstrates a minimal streaming pipeline:
- Producer: simulates delivery routes and publishes JSON events to Kafka.
- Consumer: reads from Kafka, batches events, and pushes them to a Power BI Streaming/Push dataset.

## Requirements
Install dependencies:

    pip install -r requirements.txt

## Configuration
1. Kafka client properties:
   Create `src/streaming/client.properties` with Confluent Cloud settings.

2. Power BI URL:
   Create `src/streaming/.env` containing:
   PBI_URL=https://api.powerbi.com/beta/<workspace-guid>/datasets/<dataset-guid>/rows?...&key=<your_key>

## Run Producer
Example:

    python src/streaming/producer.py --properties src/streaming/client.properties --topic RutasRealTime --interval 1 --speed 25 --repartidores 3 --start-lat 4.652 --start-lon -74.083 --delta 0.02

Events look like:
    {"repartidor":"r1","lat":4.65,"lon":-74.08,"ts":"2025-09-13T12:00:00Z"}

## Run Consumer
Example:

    python src/streaming/consumer.py --properties src/streaming/client.properties --env-file src/streaming/.env --topic RutasRealTime --batch-size 200 --batch-seconds 2

Batches events and posts to Power BI via PBI_URL.

## Tests
Run from project root:

    pytest -q tests/streaming/test_producer.py
    pytest -q tests/streaming/test_consumer.py

Tests validate config parsing, schema checks, producer logic, and push-to-PBI behavior.

## Notes
- Data format: JSON (no Avro/Schema Registry).
- Delivery semantics: at-least-once on consumer side.
- Adjust batch size/seconds to respect Power BI limits (≤10k rows per POST, ≤120 req/min).

#!/usr/bin/env python3

import os
import json
import time
import signal
import argparse
import requests
from pathlib import Path
from confluent_kafka import Consumer
from dotenv import load_dotenv

shutdown = False


def _sig(_s, _f):
    global shutdown
    shutdown = True


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def read_config(path: str) -> dict:
    cfg = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
    return cfg


def make_consumer(props_path: str) -> Consumer:
    cfg = read_config(props_path)
    cfg.setdefault("group.id", "pbi_consumer")
    cfg.setdefault("auto.offset.reset", "earliest")
    cfg.setdefault("enable.auto.commit", "true")
    return Consumer(cfg)


def valid_row(obj) -> bool:
    if not isinstance(obj, dict):
        return False
    for k in ("repartidor", "lat", "lon", "ts"):
        if k not in obj:
            return False
    return True


def push_rows(pbi_url: str, rows: list) -> bool:
    if not rows:
        return True
    if not pbi_url:
        print("PBI_URL not set (check your .env)")
        return False
    try:
        r = requests.post(pbi_url, json={"rows": rows}, timeout=10)
        if r.ok:
            return True
        print("PBI push error:", r.status_code, r.text[:200])
        return False
    except requests.RequestException as e:
        print("PBI push exception:", e)
        return False


def main():
    parser = argparse.ArgumentParser(description="Kafka → Power BI Streaming consumer (minimal).")
    parser.add_argument("--properties", default="client.properties", help="Path to client.properties")
    parser.add_argument("--topic", default="RutasRealTime", help="Kafka topic to consume")
    parser.add_argument("--batch-size", type=int, default=200, help="Max rows per POST to Power BI (≤ 10000)")
    parser.add_argument("--batch-seconds", type=float, default=2.0, help="Max seconds between POSTs")
    parser.add_argument("--env-file", default=str(Path(__file__).resolve().parents[2] / ".env"),
                        help="Path to .env file with PBI_URL")
    args = parser.parse_args()

    # Load .env (PBI_URL)
    if args.env_file:
        load_dotenv(args.env_file)

    pbi_url = os.getenv("PBI_URL")
    if not pbi_url:
        print("Warning: PBI_URL not found in environment. Set it in .env or the environment.")

    consumer = make_consumer(args.properties)
    consumer.subscribe([args.topic])

    batch = []
    last_send = time.time()

    try:
        while not shutdown:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is None:
                if batch and (now - last_send >= args.batch_seconds):
                    if push_rows(pbi_url, batch):
                        batch = []
                        last_send = now
                continue

            if msg.error():
                print("Consumer error:", msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                if valid_row(payload):
                    batch.append(payload)
                else:
                    print("Skipping invalid row:", payload)
            except Exception as e:
                print("Decode error:", e)

            # size trigger
            if len(batch) >= args.batch_size:
                if push_rows(pbi_url, batch):
                    batch = []
                    last_send = now

            # time trigger
            if batch and (now - last_send >= args.batch_seconds):
                if push_rows(pbi_url, batch):
                    batch = []
                    last_send = now

    finally:
        if batch:
            push_rows(pbi_url, batch)
        consumer.close()
        print("consumer closed")


if __name__ == "__main__":
    main()

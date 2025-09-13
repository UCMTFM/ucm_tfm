# consumer.py

import os
import json
import time
import signal
import argparse
import requests
from pathlib import Path
from confluent_kafka import Consumer
from dotenv import load_dotenv

"""
Kafka → Power BI Streaming consumer.

Reads JSON events from a Kafka topic and pushes them in small batches
to a Power BI Streaming/Push dataset (URL taken from .env → PBI_URL).
"""

# Global shutdown flag 
shutdown = False

# All relevant files are read from this folder by default
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PROPERTIES = str(BASE_DIR / "client.properties")
DEFAULT_ENV_FILE   = str(BASE_DIR / ".env")

def _sig(_s, _f):
    """Signal handler that asks the main loop to exit cleanly."""
    global shutdown
    shutdown = True

# Graceful termination on Ctrl+C or container stop
signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def read_config(path: str) -> dict:
    """
    Parse a .properties-style file into a dict.
    - Ignores blank lines and comments (#).
    - Accepts "key=value" pairs, preserves case.
    """
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
    """
    Build a Confluent Kafka Consumer from client.properties,
    adding safe defaults for a plug-and-play streaming read.
    """
    cfg = read_config(props_path)
    cfg.setdefault("group.id", "pbi_consumer")
    cfg.setdefault("auto.offset.reset", "earliest")
    cfg.setdefault("enable.auto.commit", "true")  # simple semantics
    return Consumer(cfg)


def valid_row(obj) -> bool:
    """
    Minimal schema check for the expected JSON payload.
    Required fields: repartidor, lat, lon, ts.
    """
    if not isinstance(obj, dict):
        return False
    for k in ("repartidor", "lat", "lon", "ts"):
        if k not in obj:
            return False
    return True


def push_rows(pbi_url: str, rows: list) -> bool:
    """
    POST rows to the Power BI Streaming/Push dataset.

    Returns:
        True on HTTP 2xx, False otherwise (no exceptions propagate).
    """
    if not rows:
        return True
    if not pbi_url:
        print("PBI_URL not set (check your .env in", DEFAULT_ENV_FILE, ")")
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
    """
    Argument parsing, .env loading, consumer loop, and batching logic.
    Sends a final flush on shutdown.
    """
    parser = argparse.ArgumentParser(description="Kafka → Power BI Streaming consumer (minimal).")
    parser.add_argument("--properties", default=DEFAULT_PROPERTIES, help="Path to client.properties")
    parser.add_argument("--topic", default="RutasRealTime", help="Kafka topic to consume")
    parser.add_argument("--batch-size", type=int, default=200, help="Max rows per POST to Power BI (≤ 10000)")
    parser.add_argument("--batch-seconds", type=float, default=2.0, help="Max seconds between POSTs")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help="Path to .env file with PBI_URL")
    args = parser.parse_args()

    # Load PBI_URL from .env (keeps secrets out of code)
    load_dotenv(args.env_file)
    pbi_url = os.getenv("PBI_URL")
    if not pbi_url:
        print("Warning: PBI_URL not found. Expected in", args.env_file)

    consumer = make_consumer(args.properties)
    consumer.subscribe([args.topic])

    batch = []
    last_send = time.time()

    try:
        while not shutdown:
            # Poll for one message (returns None on timeout)
            msg = consumer.poll(1.0)
            now = time.time()

            # Time-based trigger when no message arrives
            if msg is None:
                if batch and (now - last_send >= args.batch_seconds):
                    if push_rows(pbi_url, batch):
                        batch = []
                        last_send = now
                continue

            # Kafka-level error (partition EOF, etc.)
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            # Decode JSON payload and validate minimal schema
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                if valid_row(payload):
                    batch.append(payload)
                else:
                    print("Skipping invalid row:", payload)
            except Exception as e:
                print("Decode error:", e)

            # Size-based trigger
            if len(batch) >= args.batch_size:
                if push_rows(pbi_url, batch):
                    batch = []
                    last_send = now

            # Time-based trigger (while messages are flowing)
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

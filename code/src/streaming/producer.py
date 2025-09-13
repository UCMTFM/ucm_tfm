#producer.py

import argparse
import json
import math
import signal
import threading
import time
from datetime import datetime, timezone
from typing import List, Tuple, Dict

import requests
from confluent_kafka import Producer

"""
Delivery Simulation Producer → Kafka.

Simulates delivery driver location updates and streams them as JSON events
to a Kafka topic at regular intervals. Routes are generated using OSRM,
and each driver runs in its own thread with configurable speed and interval.

Configuration is loaded from a .properties file (Kafka client config),
and arguments control the number of drivers, speed, interval, topic, etc.

Intended for use with streaming pipelines (e.g., Power BI dashboards or Kafka consumers).
"""

# Configuration & Kafka Setup
def read_config(path: str) -> Dict[str, str]:
    """
    Reads a .properties configuration file (key=value per line).
    Ignores commented lines starting with #.
    Returns a dictionary usable by the Kafka producer.
    """
    cfg = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
    return cfg


def make_producer(config: Dict[str, str]) -> Producer:
    """
    Creates a Confluent Kafka Producer using the provided configuration.
    """
    return Producer(config)


def delivery_report(err, msg):
    """
    Callback function for Kafka delivery reports.
    Logs any delivery errors.
    """
    if err is not None:
        print(f"[DELIVERY-ERROR] {err} (topic={msg.topic()}, key={msg.key()}, partition={msg.partition()})")


# Route Simulation using OSRM

def osrm_route(start: Tuple[float, float], end: Tuple[float, float]) -> List[Tuple[float, float]]:
    """
    Calls the public OSRM API to compute a driving route between two coordinates.
    If the request fails, falls back to a straight line (start → end).
    Returns a list of (lat, lon) waypoints.
    """
    url = (
        f"https://router.project-osrm.org/route/v1/driving/"
        f"{start[1]},{start[0]};{end[1]},{end[0]}?overview=full&geometries=geojson"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        coords = data["routes"][0]["geometry"]["coordinates"]  # format: [ [lon, lat], ... ]
        return [(lat, lon) for lon, lat in coords]
    except Exception:
        # fallback to a straight line if the API fails
        return [start, end]


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """
    Calculates the Haversine distance in kilometers between two (lat, lon) points.
    """
    R = 6371.0  # Earth's radius in km
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def densify_polyline(points: List[Tuple[float, float]]) -> Tuple[List[Dict], float]:
    """
    Processes a list of points into segments with cumulative distances.
    Returns:
        - A list of segments with start/end points, segment length, and cumulative distance.
        - The total route distance in kilometers.
    """
    segs = []
    cum = 0.0
    for i in range(len(points) - 1):
        a, b = points[i], points[i + 1]
        d = haversine_km(a, b)
        segs.append({"start": a, "end": b, "length": d, "cumdist": cum})
        cum += d
    return segs, cum


def simulate_repartidor(
    repartidor_id: str,
    start: Tuple[float, float],
    end: Tuple[float, float],
    speed_kmh: float,
    interval_s: float,
):
    """
    Generator that simulates a delivery driver moving along a route.
    Yields position updates (lat, lon, timestamp) at regular time intervals.
    Speed and interval determine how fast and how often the points are emitted.
    """
    route = osrm_route(start, end)
    segs, total_km = densify_polyline(route)

    if total_km <= 0 or not segs:
        # Edge case: start and end are the same or route failed
        now = datetime.now(timezone.utc).isoformat()
        yield {"repartidor": repartidor_id, "lat": start[0], "lon": start[1], "ts": now}
        return

    # Total travel time in seconds
    total_h = total_km / max(speed_kmh, 1e-6)
    total_s = total_h * 3600.0
    steps = max(1, int(total_s / interval_s))

    for step in range(steps + 1):
        traveled = (step / steps) * total_km

        # Find current segment and interpolate position
        lat, lon = segs[-1]["end"]  # default to final point
        for seg in segs:
            if seg["cumdist"] + seg["length"] >= traveled:
                frac = (traveled - seg["cumdist"]) / seg["length"] if seg["length"] > 0 else 0.0
                lat1, lon1 = seg["start"]
                lat2, lon2 = seg["end"]
                lat = lat1 + frac * (lat2 - lat1)
                lon = lon1 + frac * (lon2 - lon1)
                break

        now = datetime.now(timezone.utc).isoformat()
        yield {"repartidor": repartidor_id, "lat": lat, "lon": lon, "ts": now}
        if step < steps:
            time.sleep(interval_s)


# Worker Thread & Main Program

shutdown = False  # Flag to gracefully stop simulation on signal


def _sig_handler(signum, frame):
    """
    Signal handler for SIGINT and SIGTERM.
    Triggers graceful shutdown by setting the global flag.
    """
    global shutdown
    shutdown = True
    print("\n[CTRL-C] Shutting down… (flushing producer)")


# Register signal handlers
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)


def worker_producer(
    producer: Producer,
    topic: str,
    repartidor_id: str,
    start: Tuple[float, float],
    end: Tuple[float, float],
    speed_kmh: float,
    interval_s: float,
):
    """
    Thread function for a single delivery driver.
    Simulates route and sends position updates to Kafka.
    """
    try:
        for point in simulate_repartidor(repartidor_id, start, end, speed_kmh, interval_s):
            if shutdown:
                break
            payload = json.dumps(point).encode("utf-8")
            producer.produce(topic, key=repartidor_id.encode("utf-8"), value=payload, on_delivery=delivery_report)
            # Non-blocking poll to process delivery callbacks
            producer.poll(0)
    except Exception as e:
        print(f"[WORKER][{repartidor_id}] ERROR: {e}")


def main():
    """
    Main function that parses arguments, initializes producer,
    spawns threads for each delivery driver, and handles graceful shutdown.
    """
    parser = argparse.ArgumentParser(description="Kafka producer simulating delivery driver locations (using OSRM).")
    parser.add_argument("--properties", default="client.properties", help="Path to client.properties file")
    parser.add_argument("--topic", default="RutasRealTime", help="Kafka topic to send messages to")
    parser.add_argument("--interval", type=float, default=1.0, help="Emit interval in seconds")
    parser.add_argument("--speed", type=float, default=25.0, help="Average speed in km/h")
    parser.add_argument("--repartidores", type=int, default=3, help="Number of concurrent delivery drivers to simulate")
    parser.add_argument("--start-lat", type=float, default=4.652, help="Base starting latitude")
    parser.add_argument("--start-lon", type=float, default=-74.083, help="Base starting longitude")
    parser.add_argument("--delta", type=float, default=0.02, help="Offset to generate distinct destination points")

    args = parser.parse_args()

    config = read_config(args.properties)
    producer = make_producer(config)

    base_start = (args.start_lat, args.start_lon)
    threads = []

    for i in range(args.repartidores):
        # Each delivery driver will have a unique destination based on delta
        end = (args.start_lat + (i + 1) * args.delta, args.start_lon + (i + 1) * args.delta)
        repartidor_id = f"repartidor_{i+1}"
        t = threading.Thread(
            target=worker_producer,
            args=(producer, args.topic, repartidor_id, base_start, end, args.speed, args.interval),
            daemon=True,
        )
        t.start()
        threads.append(t)
        print(f"[START] {repartidor_id} → {args.topic} (start={base_start}, end={end})")

    # Main loop: wait for threads unless shutdown signal is received
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
            if shutdown:
                break
    finally:
        print("[FLUSH] Flushing pending messages…")
        producer.flush(10)
        print("[DONE] Shutdown complete.")


if __name__ == "__main__":
    main()

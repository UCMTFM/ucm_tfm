# test_producer.py

import sys
from pathlib import Path
import json
import pytest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from streaming import producer as mod

"""
A minimal pytest suite for the streaming.producer module.
Tests include:
- Parsing configuration files with key-value pairs
- Handling OSRM routing failures with proper fallbacks
- Checking polyline math and distance calculations
- Generating position data without actual delays
- Sending messages through a mock Kafka producer and verifying delivery

These tests are designed to be quick and self-contained,
ensuring the core functionality works as expected.
"""

def test_read_config_parses_properties(tmp_path):
    """
    Ensures read_config() parses a .properties-style file:
    - Ignores comment lines
    - Trims whitespace around keys/values
    - Preserves expected key names and values
    """
    p = tmp_path / "client.properties"
    p.write_text(
        "# comment\n"
        "bootstrap.servers=abc:9092\n"
        "sasl.username = user\n"
        "sasl.password= pass \n",
        encoding="utf-8",
    )

    cfg = mod.read_config(str(p))
    assert cfg["bootstrap.servers"] == "abc:9092"
    assert cfg["sasl.username"] == "user"
    assert cfg["sasl.password"] == "pass"


def test_osrm_route_fallback(monkeypatch):
    """
    If the OSRM HTTP call fails, osrm_route() must fall back to a two-point polyline
    consisting only of the start and end coordinates.
    """
    def boom(*_, **__):
        raise RuntimeError("network down")

    # Simulate network failure in requests.get
    monkeypatch.setattr(mod.requests, "get", boom)

    start = (4.0, -74.0)
    end = (4.1, -74.1)
    pts = mod.osrm_route(start, end)
    assert pts == [start, end]


def test_densify_polyline_and_haversine_trivial():
    """
    Validates consistency between densify_polyline() and haversine_km() for a simple
    two-point line, and checks the edge case with identical points.
    """
    a = (4.0, -74.0)
    b = (4.1, -74.1)

    # Two distinct points → one segment; total distance equals haversine distance
    segs, total = mod.densify_polyline([a, b])
    assert len(segs) == 1
    assert pytest.approx(total, rel=1e-6) == mod.haversine_km(a, b)

    # Identical points → total distance is zero; if segments exist, each has zero length
    segs2, total2 = mod.densify_polyline([a, a])
    assert total2 == 0.0
    assert all(s["length"] == 0.0 for s in segs2)


def test_simulate_repartidor_no_sleep(monkeypatch):
    """
    Makes simulate_repartidor() deterministic and fast by:
    - Disabling time.sleep
    - Forcing a simple two-point "route"
    Then asserts it emits at least two points and ends at the exact destination.
    """
    # Remove real delays
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    # Avoid external OSRM dependency
    monkeypatch.setattr(mod, "osrm_route", lambda s, e: [s, e])

    start = (4.0, -74.0)
    end = (4.1, -74.1)
    pts = list(mod.simulate_repartidor("r1", start, end, speed_kmh=36.0, interval_s=1.0))

    assert len(pts) >= 2
    assert pts[0]["repartidor"] == "r1"
    assert isinstance(pts[0]["ts"], str)
    # Final emitted point must match the destination
    assert (pts[-1]["lat"], pts[-1]["lon"]) == end


def test_worker_producer_sends_messages(monkeypatch):
    """
    Verifies worker_producer() publishes messages using a fake Producer:
    - simulate_repartidor() is stubbed to return two fixed events
    - produce() receives a valid JSON payload and consistent topic/key
    - poll() is invoked to drain delivery callbacks
    """
    # Stub the generator to emit two fixed positions
    monkeypatch.setattr(
        mod, "simulate_repartidor",
        lambda *_args, **_kw: iter([
            {"repartidor": "rX", "lat": 1.0, "lon": 2.0, "ts": "t1"},
            {"repartidor": "rX", "lat": 1.1, "lon": 2.1, "ts": "t2"},
        ])
    )

    class FakeProducer:
        """
        Minimal test double for confluent_kafka.Producer.
        Captures calls to produce(), counts poll()/flush() invocations,
        and triggers on_delivery without errors.
        """
        def __init__(self):
            self.calls = []
            self.polled = 0
            self.flushed = 0

        def produce(self, topic, key, value, on_delivery=None):
            payload = json.loads(value.decode("utf-8"))
            assert payload["repartidor"] == "rX"
            self.calls.append((topic, key.decode("utf-8"), payload))
            if on_delivery:
                on_delivery(None, SimpleNamespace(
                    topic=lambda: topic, key=lambda: key, partition=lambda: 0
                ))

        def poll(self, _timeout):
            self.polled += 1

        def flush(self, _timeout=None):
            self.flushed += 1

    fake = FakeProducer()
    topic = "RutasRealTime"

    # Run the worker logic against the fake producer
    mod.worker_producer(fake, topic, "rX", (0.0, 0.0), (0.1, 0.1), 36.0, 1.0)

    # Two events should have been sent with the expected topic/key and fields
    assert len(fake.calls) == 2
    for t, key, payload in fake.calls:
        assert t == topic
        assert key == "rX"
        assert {"lat", "lon", "ts"} <= set(payload.keys())

    # Ensure the code drained delivery callbacks at least once
    assert fake.polled >= 1

# test_consumer.py

import sys
from pathlib import Path
import json
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from streaming import consumer as mod

"""
A minimal pytest suite for the streaming.consumer module.
Tests include:
- Parsing configuration files with key-value pairs
- Validating the structure and contents of incoming JSON rows
- Verifying successful and failed HTTP POST requests to the Power BI API
- Ensuring push_rows behaves correctly with empty batches or missing URLs
- Handling request exceptions gracefully

These tests are designed to be fast, isolated, and cover both success
and error paths for key consumer functionalities.
"""


def test_read_config_parses_properties(tmp_path):
    """
    Ensures read_config() correctly parses a .properties-style file:
    - Ignores comment lines
    - Trims whitespace around keys and values
    - Preserves key-value mapping as expected
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


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"repartidor": "r1", "lat": 1.0, "lon": 2.0, "ts": "t"}, True),
        ({}, False),
        ({"repartidor": "r1", "lat": 1.0, "lon": 2.0}, False),
        ("not-a-dict", False),
    ],
)
def test_valid_row(payload, expected):
    """
    Tests valid_row() to ensure only well-formed payloads are accepted.
    - Must be a dict
    - Must contain 'repartidor', 'lat', 'lon', and 'ts'
    """
    assert mod.valid_row(payload) is expected


def test_push_rows_success(monkeypatch):
    """
    Ensures push_rows() makes a valid POST request and returns True when:
    - The HTTP response is OK (200)
    - The payload is well-formed with a 'rows' field
    """
    class OkResp:
        ok = True
        status_code = 200
        text = "ok"

    calls = {}

    def fake_post(url, json=None, timeout=None):
        assert isinstance(json, dict)
        assert "rows" in json
        assert isinstance(json["rows"], list)
        calls["n"] = calls.get("n", 0) + 1
        return OkResp()

    monkeypatch.setattr(mod.requests, "post", fake_post)
    assert mod.push_rows("http://pbi", [{"a": 1}]) is True
    assert calls["n"] == 1


def test_push_rows_http_error(monkeypatch):
    """
    Simulates an HTTP error response (e.g., throttling) and
    asserts push_rows() returns False when response.ok is False.
    """
    class BadResp:
        ok = False
        status_code = 429
        text = "throttled"

    def fake_post(url, json=None, timeout=None):
        return BadResp()

    monkeypatch.setattr(mod.requests, "post", fake_post)
    assert mod.push_rows("http://pbi", [{"a": 1}]) is False


def test_push_rows_exception(monkeypatch):
    """
    Simulates an exception (e.g., network failure) during the POST request.
    Ensures push_rows() catches the exception and returns False.
    """
    def boom(*_, **__):
        raise mod.requests.RequestException("net down")

    monkeypatch.setattr(mod.requests, "post", boom)
    assert mod.push_rows("http://pbi", [{"a": 1}]) is False


def test_push_rows_empty_batch_noop():
    """
    Ensures push_rows() returns True without making any HTTP call
    when the input batch is empty.
    """
    assert mod.push_rows("http://pbi", []) is True


def test_push_rows_missing_url():
    """
    Ensures push_rows() returns False if the URL is empty or missing.
    Prevents attempts to send data without a target endpoint.
    """
    assert mod.push_rows("", [{"x": 1}]) is False

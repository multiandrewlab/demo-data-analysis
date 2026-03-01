# tests/test_mysql_connection.py
import time
import pytest
from unittest.mock import MagicMock
from src.connections.mysql import MySQLConnection
from src.config import MySQLConfig


def test_rate_limiter_enforces_sleep():
    """Rate limiter sleeps between queries."""
    config = MySQLConfig(host="localhost", max_concurrent_queries=1,
                         sleep_between_queries_ms=200)
    conn = MySQLConnection(config)

    # Mock the engine to avoid actual DB connection
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [{"id": 1}]
    mock_result.keys.return_value = ["id"]
    mock_connection.execute.return_value = mock_result
    mock_connection.__enter__ = lambda s: mock_connection
    mock_connection.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_connection

    conn._engine = mock_engine

    start = time.monotonic()
    conn.execute_query("SELECT 1")
    t1 = time.monotonic() - start

    conn.execute_query("SELECT 2")
    t2 = time.monotonic() - start

    # Second query should be delayed by at least sleep_between_queries_ms
    assert t2 - t1 >= 0.15  # 200ms minus some tolerance


def test_semaphore_limits_concurrency():
    """Concurrent queries are limited by semaphore."""
    config = MySQLConfig(host="localhost", max_concurrent_queries=1,
                         sleep_between_queries_ms=0)
    conn = MySQLConnection(config)
    assert conn._semaphore._value == 1


def test_build_connection_url():
    """Connection URL is built correctly from config."""
    config = MySQLConfig(host="db.example.com", port=3307, user="analyst",
                         password="secret")
    conn = MySQLConnection(config)
    url = conn._build_url(config, "mydb")
    assert "db.example.com" in url
    assert "3307" in url
    assert "analyst" in url

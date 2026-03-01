# tests/test_bigquery_connection.py
import pytest
from unittest.mock import MagicMock
from src.connections.bigquery import BigQueryConnection, QueryTooExpensiveError
from src.config import BigQueryConfig


def test_dry_run_skips_expensive_query():
    """Queries exceeding dry_run_threshold_bytes raise QueryTooExpensiveError."""
    config = BigQueryConfig(dry_run_threshold_bytes=100_000_000,
                            max_bytes_per_query=500_000_000)
    conn = BigQueryConnection(config, project="test-project")

    # Mock the BQ client
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.total_bytes_processed = 200_000_000  # exceeds threshold
    mock_client.query.return_value = mock_job
    conn._client = mock_client

    with pytest.raises(QueryTooExpensiveError) as exc_info:
        conn.execute_query("SELECT * FROM huge_table")
    assert "200000000" in str(exc_info.value) or "200,000,000" in str(exc_info.value)


def test_dry_run_allows_cheap_query():
    """Queries under threshold proceed to actual execution."""
    config = BigQueryConfig(dry_run_threshold_bytes=100_000_000,
                            max_bytes_per_query=500_000_000)
    conn = BigQueryConnection(config, project="test-project")

    mock_client = MagicMock()
    # Dry-run returns low byte count
    mock_dry_job = MagicMock()
    mock_dry_job.total_bytes_processed = 50_000_000
    # Actual query returns results
    mock_result = MagicMock()
    mock_result.to_dataframe.return_value.__iter__ = MagicMock(return_value=iter([]))
    mock_result.total_rows = 0

    mock_client.query.side_effect = [mock_dry_job, mock_result]
    conn._client = mock_client

    result = conn.execute_query("SELECT 1")
    # Should have been called twice: dry-run + actual
    assert mock_client.query.call_count == 2


def test_max_bytes_billed_is_set():
    """maximumBytesBilled is set on every actual query job config."""
    config = BigQueryConfig(dry_run_threshold_bytes=1_000_000_000,
                            max_bytes_per_query=500_000_000)
    conn = BigQueryConnection(config, project="test-project")

    mock_client = MagicMock()
    mock_dry_job = MagicMock()
    mock_dry_job.total_bytes_processed = 100
    mock_result = MagicMock()
    mock_result.total_rows = 0
    mock_client.query.side_effect = [mock_dry_job, mock_result]
    conn._client = mock_client

    conn.execute_query("SELECT 1")

    # Second call is the actual query — check its job_config
    actual_call = mock_client.query.call_args_list[1]
    job_config = actual_call.kwargs.get("job_config")
    assert job_config is not None
    assert job_config.maximum_bytes_billed == 500_000_000

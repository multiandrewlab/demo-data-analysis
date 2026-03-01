# tests/test_sampler.py
import pytest
from src.profiling.sampler import AdaptiveSampler
from src.config import SamplingConfig


@pytest.fixture
def sampler():
    return AdaptiveSampler(SamplingConfig())


def test_small_table_no_sampling(sampler):
    """Tables under small_table_threshold get no sampling clause."""
    query = sampler.build_sample_query("mysql", "analytics", "small_table", row_count=50_000)
    assert "SELECT *" in query
    assert "LIMIT" not in query
    assert "TABLESAMPLE" not in query


def test_medium_mysql_table_uses_modulo(sampler):
    """Medium MySQL tables use modulo-based sampling on primary key."""
    query = sampler.build_sample_query("mysql", "analytics", "medium_table",
                                       row_count=1_000_000, pk_column="id")
    assert "MOD" in query or "%" in query
    assert "id" in query


def test_large_mysql_table_uses_limit(sampler):
    """Large MySQL tables use LIMIT with a fixed row count."""
    query = sampler.build_sample_query("mysql", "analytics", "huge_table",
                                       row_count=50_000_000, pk_column="id")
    assert "100000" in query  # large_table_sample_rows default


def test_medium_bq_table_uses_tablesample(sampler):
    """Medium BigQuery tables use TABLESAMPLE."""
    query = sampler.build_sample_query("bigquery", "dataset", "medium_table",
                                       row_count=1_000_000)
    assert "TABLESAMPLE" in query
    assert "SYSTEM" in query
    assert "10 PERCENT" in query


def test_large_bq_table_uses_tablesample(sampler):
    """Large BigQuery tables use TABLESAMPLE with calculated percentage."""
    config = SamplingConfig(large_table_sample_rows=100_000)
    s = AdaptiveSampler(config)
    query = s.build_sample_query("bigquery", "dataset", "huge_table",
                                  row_count=100_000_000)
    assert "TABLESAMPLE" in query


def test_custom_columns(sampler):
    """Can specify which columns to select."""
    query = sampler.build_sample_query("mysql", "db", "tbl", row_count=50,
                                       columns=["country", "clicks", "impressions"])
    assert "country" in query
    assert "clicks" in query
    assert "SELECT *" not in query

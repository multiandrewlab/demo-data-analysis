# tests/test_schema_discovery.py
import tempfile
import os
import pytest
from unittest.mock import MagicMock
from src.discovery.schema import SchemaDiscoverer
from src.config import ProfilerConfig, ClassificationConfig
from src.storage.store import ProfileStore


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    s = ProfileStore(db_path)
    yield s
    s.close()
    os.unlink(db_path)


def _mock_mysql_info_schema():
    """Return mock INFORMATION_SCHEMA data for MySQL."""
    return {
        "tables": [
            {"TABLE_NAME": "ad_events", "TABLE_ROWS": 500000, "TABLE_TYPE": "BASE TABLE"},
            {"TABLE_NAME": "publishers", "TABLE_ROWS": 100, "TABLE_TYPE": "BASE TABLE"},
        ],
        "columns": [
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "id", "DATA_TYPE": "bigint",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "PRI", "COLUMN_TYPE": "bigint", "EXTRA": "auto_increment"},
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "publisher_id", "DATA_TYPE": "int",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "MUL", "COLUMN_TYPE": "int", "EXTRA": ""},
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "country", "DATA_TYPE": "varchar",
             "IS_NULLABLE": "YES", "COLUMN_KEY": "", "COLUMN_TYPE": "varchar(2)", "EXTRA": ""},
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "impressions", "DATA_TYPE": "int",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "", "COLUMN_TYPE": "int", "EXTRA": ""},
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "clicks", "DATA_TYPE": "int",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "", "COLUMN_TYPE": "int", "EXTRA": ""},
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "event_date", "DATA_TYPE": "date",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "", "COLUMN_TYPE": "date", "EXTRA": ""},
            {"TABLE_NAME": "publishers", "COLUMN_NAME": "id", "DATA_TYPE": "int",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "PRI", "COLUMN_TYPE": "int", "EXTRA": ""},
            {"TABLE_NAME": "publishers", "COLUMN_NAME": "name", "DATA_TYPE": "varchar",
             "IS_NULLABLE": "NO", "COLUMN_KEY": "", "COLUMN_TYPE": "varchar(255)", "EXTRA": ""},
        ],
        "foreign_keys": [
            {"TABLE_NAME": "ad_events", "COLUMN_NAME": "publisher_id",
             "REFERENCED_TABLE_NAME": "publishers", "REFERENCED_COLUMN_NAME": "id"},
        ],
    }


def test_discover_mysql_tables(store):
    """Discovery processes INFORMATION_SCHEMA and stores tables+columns."""
    mock_mysql = MagicMock()
    mock_mysql.get_information_schema.return_value = _mock_mysql_info_schema()

    config = ProfilerConfig()
    discoverer = SchemaDiscoverer(config, store, mysql_conn=mock_mysql)
    discoverer.discover_mysql("analytics")

    tables = store.get_all_tables()
    assert len(tables) == 2

    ad_events = next(t for t in tables if t.table_name == "ad_events")
    assert ad_events.row_count == 500000
    assert ad_events.source == "mysql"
    assert ad_events.discovery_status == "completed"

    # Check columns were classified
    cols = store.get_columns_for_table(ad_events.id)
    col_map = {c.column_name: c for c in cols}
    assert col_map["id"].classification == "identifier"
    assert col_map["id"].is_primary_key is True
    assert col_map["country"].classification == "dimension"
    assert col_map["impressions"].classification == "metric"
    assert col_map["clicks"].classification == "metric"
    assert col_map["event_date"].classification == "temporal"
    assert col_map["publisher_id"].classification == "identifier"
    assert col_map["publisher_id"].is_foreign_key is True
    assert col_map["publisher_id"].fk_target_table == "publishers"


def test_discover_is_idempotent(store):
    """Running discovery twice doesn't create duplicate tables."""
    mock_mysql = MagicMock()
    mock_mysql.get_information_schema.return_value = _mock_mysql_info_schema()

    config = ProfilerConfig()
    discoverer = SchemaDiscoverer(config, store, mysql_conn=mock_mysql)
    discoverer.discover_mysql("analytics")
    discoverer.discover_mysql("analytics")

    tables = store.get_all_tables()
    assert len(tables) == 2  # not 4

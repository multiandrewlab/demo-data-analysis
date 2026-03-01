# tests/test_storage_store.py
import tempfile
import os
import pytest
from src.storage.store import ProfileStore


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    s = ProfileStore(db_path)
    yield s
    s.close()
    os.unlink(db_path)


def test_upsert_table(store):
    """upsert_table creates a new table record and returns its id."""
    table_id = store.upsert_table("mysql", "analytics", "ad_events", row_count=500_000)
    assert table_id is not None
    tbl = store.get_table(table_id)
    assert tbl.source == "mysql"
    assert tbl.table_name == "ad_events"
    assert tbl.row_count == 500_000
    assert tbl.discovery_status == "completed"


def test_upsert_table_updates_existing(store):
    """upsert_table updates row_count if table already exists."""
    id1 = store.upsert_table("mysql", "analytics", "ad_events", row_count=100)
    id2 = store.upsert_table("mysql", "analytics", "ad_events", row_count=200)
    assert id1 == id2
    tbl = store.get_table(id1)
    assert tbl.row_count == 200


def test_add_column(store):
    """add_column creates a column linked to a table."""
    table_id = store.upsert_table("mysql", "analytics", "ad_events")
    col_id = store.add_column(table_id, "country", "VARCHAR(2)", classification="dimension")
    col = store.get_column(col_id)
    assert col.column_name == "country"
    assert col.classification == "dimension"
    assert col.table_id == table_id


def test_update_table_status(store):
    """update_table_status changes the status of a phase."""
    table_id = store.upsert_table("mysql", "analytics", "ad_events")
    store.update_table_status(table_id, "profile_status", "completed")
    tbl = store.get_table(table_id)
    assert tbl.profile_status == "completed"


def test_get_tables_by_status(store):
    """get_tables_by_status filters tables correctly."""
    store.upsert_table("mysql", "db", "t1")
    id2 = store.upsert_table("mysql", "db", "t2")
    store.update_table_status(id2, "profile_status", "completed")

    pending = store.get_tables_by_status("profile_status", "pending")
    assert len(pending) == 1
    assert pending[0].table_name == "t1"


def test_save_column_profile(store):
    """save_column_profile stores statistical data for a column."""
    table_id = store.upsert_table("mysql", "db", "t1")
    col_id = store.add_column(table_id, "clicks", "INT", classification="metric")
    store.save_column_profile(col_id, null_rate=0.01, mean=45.2, stddev=12.3,
                              distinct_count=1000, min_value="0", max_value="500")
    profile = store.get_column_profile(col_id)
    assert profile.null_rate == pytest.approx(0.01)
    assert profile.mean == pytest.approx(45.2)


def test_save_ratio(store):
    """save_ratio stores a ratio between two metric columns."""
    table_id = store.upsert_table("mysql", "db", "t1")
    num_id = store.add_column(table_id, "clicks", "INT", classification="metric")
    den_id = store.add_column(table_id, "impressions", "INT", classification="metric")
    ratio_id = store.save_ratio(table_id, num_id, den_id, global_ratio=0.032, ratio_stddev=0.01)
    assert ratio_id is not None


def test_save_dimensional_breakdown(store):
    """save_dimensional_breakdown stores ratio values per dimension value."""
    table_id = store.upsert_table("mysql", "db", "t1")
    num_id = store.add_column(table_id, "clicks", "INT", classification="metric")
    den_id = store.add_column(table_id, "impressions", "INT", classification="metric")
    dim_id = store.add_column(table_id, "country", "VARCHAR", classification="dimension")
    ratio_id = store.save_ratio(table_id, num_id, den_id, global_ratio=0.032)
    store.save_dimensional_breakdown(ratio_id, dim_id, "US", ratio_value=0.045, sample_size=10000)
    store.save_dimensional_breakdown(ratio_id, dim_id, "GB", ratio_value=0.028, sample_size=5000)
    breakdowns = store.get_breakdowns_for_ratio(ratio_id)
    assert len(breakdowns) == 2
    assert {b.dimension_value for b in breakdowns} == {"US", "GB"}

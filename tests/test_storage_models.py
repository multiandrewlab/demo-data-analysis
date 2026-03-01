# tests/test_storage_models.py
import tempfile
import os
import pytest
from sqlalchemy import create_engine, inspect
from src.storage.models import Base, create_database


def test_create_database_creates_all_tables():
    """create_database should create all expected tables in SQLite."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        engine = create_database(db_path)
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())

        expected = {
            "tables", "columns", "column_profiles", "ratios",
            "dimensional_breakdowns", "cross_table_relationships",
            "temporal_trends",
        }
        assert expected.issubset(table_names), f"Missing tables: {expected - table_names}"
    finally:
        os.unlink(db_path)


def test_tables_table_has_expected_columns():
    """The 'tables' table should have all required columns."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        engine = create_database(db_path)
        inspector = inspect(engine)
        col_names = {c["name"] for c in inspector.get_columns("tables")}
        expected = {"id", "source", "database_name", "table_name", "row_count",
                    "discovery_status", "profile_status", "ratio_status"}
        assert expected.issubset(col_names)
    finally:
        os.unlink(db_path)

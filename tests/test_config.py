# tests/test_config.py
import os
import tempfile
import pytest
import yaml
from src.config import load_config, ProfilerConfig


def test_load_config_from_file():
    """Config loads from a YAML file and provides typed access."""
    cfg_data = {
        "mysql": {"host": "db.example.com", "port": 3306, "max_concurrent_queries": 3},
        "bigquery": {"project": "my-project", "max_bytes_per_query": 500_000_000},
        "sampling": {"small_table_threshold": 50_000},
        "classification": {"max_dimension_cardinality": 500},
        "output": {"database_path": "/tmp/test.db"},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg_data, f)
        path = f.name

    try:
        config = load_config(path)
        assert isinstance(config, ProfilerConfig)
        assert config.mysql.host == "db.example.com"
        assert config.mysql.max_concurrent_queries == 3
        assert config.bigquery.project == "my-project"
        assert config.bigquery.max_bytes_per_query == 500_000_000
        assert config.sampling.small_table_threshold == 50_000
        assert config.classification.max_dimension_cardinality == 500
        assert config.output.database_path == "/tmp/test.db"
    finally:
        os.unlink(path)


def test_load_config_defaults():
    """Missing keys use sensible defaults."""
    cfg_data = {"mysql": {"host": "localhost"}, "output": {"database_path": "/tmp/t.db"}}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg_data, f)
        path = f.name

    try:
        config = load_config(path)
        assert config.mysql.max_concurrent_queries == 2
        assert config.mysql.sleep_between_queries_ms == 500
        assert config.sampling.small_table_threshold == 100_000
        assert config.classification.max_dimension_cardinality == 1000
    finally:
        os.unlink(path)


def test_load_config_env_override():
    """Environment variables override YAML values for sensitive fields."""
    cfg_data = {"mysql": {"host": "localhost", "password": "from_yaml"}, "output": {"database_path": "/tmp/t.db"}}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg_data, f)
        path = f.name

    try:
        os.environ["PROFILER_MYSQL_PASSWORD"] = "from_env"
        config = load_config(path)
        assert config.mysql.password == "from_env"
    finally:
        os.environ.pop("PROFILER_MYSQL_PASSWORD", None)
        os.unlink(path)

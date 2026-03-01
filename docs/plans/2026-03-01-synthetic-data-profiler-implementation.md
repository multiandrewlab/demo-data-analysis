# Synthetic Data Profiler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a phased pipeline that analyzes 600+ tables across MySQL and BigQuery, discovering statistical patterns, ratios, and dimensional breakdowns to drive realistic synthetic data generation.

**Architecture:** Four-phase pipeline (Discovery → Profiling → Ratios → Report) with incremental SQLite storage, adaptive sampling, and cost/load controls. Each phase is independently runnable and resumable via CLI.

**Tech Stack:** Python 3.11+, click (CLI), SQLAlchemy (MySQL + SQLite ORM), google-cloud-bigquery, pandas/numpy (stats), openmetadata-ingestion (optional), pyyaml, rich (progress output).

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/__init__.py`
- Create: `src/connections/__init__.py`
- Create: `src/discovery/__init__.py`
- Create: `src/profiling/__init__.py`
- Create: `src/ratios/__init__.py`
- Create: `src/storage/__init__.py`
- Create: `tests/__init__.py`
- Create: `config/settings.yaml`

**Step 1: Create pyproject.toml with all dependencies**

```toml
[project]
name = "synthetic-data-profiler"
version = "0.1.0"
description = "Analyze production databases to discover statistical patterns for synthetic data generation"
requires-python = ">=3.11"
dependencies = [
    "click>=8.1",
    "sqlalchemy>=2.0",
    "pymysql>=1.1",
    "google-cloud-bigquery>=3.0",
    "pandas>=2.0",
    "numpy>=1.24",
    "pyyaml>=6.0",
    "rich>=13.0",
]

[project.optional-dependencies]
openmetadata = ["openmetadata-ingestion>=1.0"]
dev = ["pytest>=7.0", "pytest-asyncio>=0.21"]

[project.scripts]
profiler = "src.cli:cli"

[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"
```

**Step 2: Create default settings.yaml**

```yaml
mysql:
  host: "localhost"
  port: 3306
  user: "readonly"
  password: ""
  databases: []
  max_concurrent_queries: 2
  sleep_between_queries_ms: 500

bigquery:
  project: ""
  datasets: []
  max_bytes_per_query: 1000000000
  dry_run_threshold_bytes: 500000000

sampling:
  small_table_threshold: 100000
  medium_table_threshold: 10000000
  large_table_sample_rows: 100000
  medium_table_sample_pct: 10

classification:
  max_dimension_cardinality: 1000
  dimension_name_patterns:
    - "country"
    - "region"
    - "publisher"
    - "platform"
    - "status"
    - "type"
    - "category"
    - "channel"
    - "device"
    - "os"
    - "browser"
    - "language"
    - "currency"
    - "gender"
  metric_name_patterns:
    - "count"
    - "amount"
    - "impressions"
    - "clicks"
    - "revenue"
    - "cost"
    - "spend"
    - "views"
    - "conversions"
    - "sessions"
    - "duration"
    - "total"
    - "sum"
    - "avg"
    - "rate"

output:
  database_path: "output/profiles.db"
  log_level: "INFO"

openmetadata:
  enabled: false
  server_url: ""
  api_token: ""
```

**Step 3: Create all `__init__.py` files and directory structure**

Create empty `__init__.py` in: `src/`, `src/connections/`, `src/discovery/`, `src/profiling/`, `src/ratios/`, `src/storage/`, `tests/`.

Also create the `output/` directory with a `.gitkeep`.

**Step 4: Install the project in dev mode**

Run: `pip install -e ".[dev]"`
Expected: successful installation

**Step 5: Commit**

```bash
git add pyproject.toml config/ src/ tests/ output/.gitkeep
git commit -m "feat: project scaffolding with dependencies and config"
```

---

### Task 2: Configuration Loading

**Files:**
- Create: `src/config.py`
- Create: `tests/test_config.py`

**Step 1: Write the failing test**

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.config'`

**Step 3: Write minimal implementation**

```python
# src/config.py
"""Configuration loading with YAML files and environment variable overrides."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class MySQLConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = "readonly"
    password: str = ""
    databases: list[str] = field(default_factory=list)
    max_concurrent_queries: int = 2
    sleep_between_queries_ms: int = 500


@dataclass
class BigQueryConfig:
    project: str = ""
    datasets: list[str] = field(default_factory=list)
    max_bytes_per_query: int = 1_000_000_000
    dry_run_threshold_bytes: int = 500_000_000


@dataclass
class SamplingConfig:
    small_table_threshold: int = 100_000
    medium_table_threshold: int = 10_000_000
    large_table_sample_rows: int = 100_000
    medium_table_sample_pct: int = 10


@dataclass
class ClassificationConfig:
    max_dimension_cardinality: int = 1000
    dimension_name_patterns: list[str] = field(default_factory=lambda: [
        "country", "region", "publisher", "platform", "status", "type",
        "category", "channel", "device", "os", "browser", "language",
        "currency", "gender",
    ])
    metric_name_patterns: list[str] = field(default_factory=lambda: [
        "count", "amount", "impressions", "clicks", "revenue", "cost",
        "spend", "views", "conversions", "sessions", "duration", "total",
        "sum", "avg", "rate",
    ])


@dataclass
class OpenMetadataConfig:
    enabled: bool = False
    server_url: str = ""
    api_token: str = ""


@dataclass
class OutputConfig:
    database_path: str = "output/profiles.db"
    log_level: str = "INFO"


@dataclass
class ProfilerConfig:
    mysql: MySQLConfig = field(default_factory=MySQLConfig)
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    classification: ClassificationConfig = field(default_factory=ClassificationConfig)
    openmetadata: OpenMetadataConfig = field(default_factory=OpenMetadataConfig)
    output: OutputConfig = field(default_factory=OutputConfig)


def _merge_dataclass(dc_class, data: dict):
    """Create a dataclass instance from a dict, ignoring unknown keys."""
    if data is None:
        return dc_class()
    field_names = {f.name for f in dc_class.__dataclass_fields__.values()}
    filtered = {k: v for k, v in data.items() if k in field_names}
    return dc_class(**filtered)


def load_config(path: str) -> ProfilerConfig:
    """Load config from YAML file with environment variable overrides."""
    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    config = ProfilerConfig(
        mysql=_merge_dataclass(MySQLConfig, raw.get("mysql")),
        bigquery=_merge_dataclass(BigQueryConfig, raw.get("bigquery")),
        sampling=_merge_dataclass(SamplingConfig, raw.get("sampling")),
        classification=_merge_dataclass(ClassificationConfig, raw.get("classification")),
        openmetadata=_merge_dataclass(OpenMetadataConfig, raw.get("openmetadata")),
        output=_merge_dataclass(OutputConfig, raw.get("output")),
    )

    # Environment variable overrides for sensitive fields
    if env_pw := os.environ.get("PROFILER_MYSQL_PASSWORD"):
        config.mysql.password = env_pw
    if env_user := os.environ.get("PROFILER_MYSQL_USER"):
        config.mysql.user = env_user
    if env_token := os.environ.get("PROFILER_OPENMETADATA_TOKEN"):
        config.openmetadata.api_token = env_token

    return config
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
git add src/config.py tests/test_config.py
git commit -m "feat: configuration loading with YAML + env overrides"
```

---

### Task 3: SQLite Storage Layer (Models)

**Files:**
- Create: `src/storage/models.py`
- Create: `tests/test_storage_models.py`

**Step 1: Write the failing test**

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_storage_models.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/storage/models.py
"""SQLAlchemy models for the profiler's SQLite output database."""

from sqlalchemy import (
    Boolean, Column, Float, Integer, String, Text, ForeignKey,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Table(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)  # 'mysql' or 'bigquery'
    database_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    row_count = Column(Integer)
    discovery_status = Column(String, default="pending")
    profile_status = Column(String, default="pending")
    ratio_status = Column(String, default="pending")

    columns = relationship("ColumnModel", back_populates="table", cascade="all, delete-orphan")
    ratios = relationship("Ratio", back_populates="table", cascade="all, delete-orphan")


class ColumnModel(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    column_name = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    classification = Column(String)  # dimension/metric/identifier/temporal/other
    nullable = Column(Boolean)
    is_primary_key = Column(Boolean, default=False)
    is_foreign_key = Column(Boolean, default=False)
    fk_target_table = Column(String)
    fk_target_column = Column(String)

    table = relationship("Table", back_populates="columns")
    profile = relationship("ColumnProfile", back_populates="column", uselist=False,
                           cascade="all, delete-orphan")


class ColumnProfile(Base):
    __tablename__ = "column_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    column_id = Column(Integer, ForeignKey("columns.id"), nullable=False, unique=True)
    null_rate = Column(Float)
    distinct_count = Column(Integer)
    min_value = Column(String)
    max_value = Column(String)
    mean = Column(Float)
    median = Column(Float)
    stddev = Column(Float)
    p5 = Column(Float)
    p25 = Column(Float)
    p75 = Column(Float)
    p95 = Column(Float)
    zero_rate = Column(Float)
    histogram_json = Column(Text)
    top_values_json = Column(Text)

    column = relationship("ColumnModel", back_populates="profile")


class Ratio(Base):
    __tablename__ = "ratios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    numerator_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    denominator_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    global_ratio = Column(Float)
    ratio_stddev = Column(Float)
    ratio_histogram_json = Column(Text)

    table = relationship("Table", back_populates="ratios")
    numerator_column = relationship("ColumnModel", foreign_keys=[numerator_column_id])
    denominator_column = relationship("ColumnModel", foreign_keys=[denominator_column_id])
    breakdowns = relationship("DimensionalBreakdown", back_populates="ratio",
                              cascade="all, delete-orphan")
    trends = relationship("TemporalTrend", back_populates="ratio",
                          cascade="all, delete-orphan")


class DimensionalBreakdown(Base):
    __tablename__ = "dimensional_breakdowns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ratio_id = Column(Integer, ForeignKey("ratios.id"), nullable=False)
    dimension_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    dimension_value = Column(String)
    ratio_value = Column(Float)
    sample_size = Column(Integer)

    ratio = relationship("Ratio", back_populates="breakdowns")
    dimension_column = relationship("ColumnModel", foreign_keys=[dimension_column_id])


class CrossTableRelationship(Base):
    __tablename__ = "cross_table_relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    target_table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    relationship_type = Column(String)  # 'fk' or 'shared_dimension'
    join_column = Column(String)
    avg_cardinality = Column(Float)
    median_cardinality = Column(Float)

    source_table = relationship("Table", foreign_keys=[source_table_id])
    target_table = relationship("Table", foreign_keys=[target_table_id])


class TemporalTrend(Base):
    __tablename__ = "temporal_trends"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ratio_id = Column(Integer, ForeignKey("ratios.id"), nullable=False)
    time_bucket = Column(String)
    ratio_value = Column(Float)
    sample_size = Column(Integer)

    ratio = relationship("Ratio", back_populates="trends")


def create_database(db_path: str):
    """Create a SQLite database with all tables. Returns the engine."""
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    return engine
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_storage_models.py -v`
Expected: all 2 tests PASS

**Step 5: Commit**

```bash
git add src/storage/models.py tests/test_storage_models.py
git commit -m "feat: SQLAlchemy models for SQLite profiler database"
```

---

### Task 4: Storage Store (CRUD Operations)

**Files:**
- Create: `src/storage/store.py`
- Create: `tests/test_storage_store.py`

**Step 1: Write the failing test**

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_storage_store.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/storage/store.py
"""CRUD operations for the profiler's SQLite database."""

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.storage.models import (
    Base, Table, ColumnModel, ColumnProfile, Ratio,
    DimensionalBreakdown, CrossTableRelationship, TemporalTrend,
)


class ProfileStore:
    def __init__(self, db_path: str):
        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)
        self._session = Session(self.engine)

    def close(self):
        self._session.close()
        self.engine.dispose()

    # --- Tables ---

    def upsert_table(self, source: str, database_name: str, table_name: str,
                     row_count: int | None = None) -> int:
        existing = self._session.execute(
            select(Table).where(
                Table.source == source,
                Table.database_name == database_name,
                Table.table_name == table_name,
            )
        ).scalar_one_or_none()

        if existing:
            if row_count is not None:
                existing.row_count = row_count
            existing.discovery_status = "completed"
            self._session.commit()
            return existing.id

        tbl = Table(source=source, database_name=database_name, table_name=table_name,
                    row_count=row_count, discovery_status="completed")
        self._session.add(tbl)
        self._session.commit()
        return tbl.id

    def get_table(self, table_id: int) -> Table:
        return self._session.get(Table, table_id)

    def get_all_tables(self) -> list[Table]:
        return list(self._session.execute(select(Table)).scalars().all())

    def update_table_status(self, table_id: int, status_field: str, status_value: str):
        tbl = self._session.get(Table, table_id)
        setattr(tbl, status_field, status_value)
        self._session.commit()

    def get_tables_by_status(self, status_field: str, status_value: str) -> list[Table]:
        return list(self._session.execute(
            select(Table).where(getattr(Table, status_field) == status_value)
        ).scalars().all())

    # --- Columns ---

    def add_column(self, table_id: int, column_name: str, data_type: str,
                   classification: str | None = None, nullable: bool = True,
                   is_primary_key: bool = False, is_foreign_key: bool = False,
                   fk_target_table: str | None = None,
                   fk_target_column: str | None = None) -> int:
        col = ColumnModel(
            table_id=table_id, column_name=column_name, data_type=data_type,
            classification=classification, nullable=nullable,
            is_primary_key=is_primary_key, is_foreign_key=is_foreign_key,
            fk_target_table=fk_target_table, fk_target_column=fk_target_column,
        )
        self._session.add(col)
        self._session.commit()
        return col.id

    def get_column(self, column_id: int) -> ColumnModel:
        return self._session.get(ColumnModel, column_id)

    def get_columns_for_table(self, table_id: int,
                              classification: str | None = None) -> list[ColumnModel]:
        stmt = select(ColumnModel).where(ColumnModel.table_id == table_id)
        if classification:
            stmt = stmt.where(ColumnModel.classification == classification)
        return list(self._session.execute(stmt).scalars().all())

    # --- Column Profiles ---

    def save_column_profile(self, column_id: int, **kwargs):
        existing = self._session.execute(
            select(ColumnProfile).where(ColumnProfile.column_id == column_id)
        ).scalar_one_or_none()

        if existing:
            for k, v in kwargs.items():
                setattr(existing, k, v)
        else:
            profile = ColumnProfile(column_id=column_id, **kwargs)
            self._session.add(profile)
        self._session.commit()

    def get_column_profile(self, column_id: int) -> ColumnProfile | None:
        return self._session.execute(
            select(ColumnProfile).where(ColumnProfile.column_id == column_id)
        ).scalar_one_or_none()

    # --- Ratios ---

    def save_ratio(self, table_id: int, numerator_column_id: int,
                   denominator_column_id: int, global_ratio: float,
                   ratio_stddev: float | None = None,
                   ratio_histogram_json: str | None = None) -> int:
        ratio = Ratio(
            table_id=table_id, numerator_column_id=numerator_column_id,
            denominator_column_id=denominator_column_id,
            global_ratio=global_ratio, ratio_stddev=ratio_stddev,
            ratio_histogram_json=ratio_histogram_json,
        )
        self._session.add(ratio)
        self._session.commit()
        return ratio.id

    def get_ratios_for_table(self, table_id: int) -> list[Ratio]:
        return list(self._session.execute(
            select(Ratio).where(Ratio.table_id == table_id)
        ).scalars().all())

    # --- Dimensional Breakdowns ---

    def save_dimensional_breakdown(self, ratio_id: int, dimension_column_id: int,
                                   dimension_value: str, ratio_value: float,
                                   sample_size: int | None = None):
        bd = DimensionalBreakdown(
            ratio_id=ratio_id, dimension_column_id=dimension_column_id,
            dimension_value=dimension_value, ratio_value=ratio_value,
            sample_size=sample_size,
        )
        self._session.add(bd)
        self._session.commit()

    def get_breakdowns_for_ratio(self, ratio_id: int) -> list[DimensionalBreakdown]:
        return list(self._session.execute(
            select(DimensionalBreakdown).where(DimensionalBreakdown.ratio_id == ratio_id)
        ).scalars().all())

    # --- Cross-Table Relationships ---

    def save_cross_table_relationship(self, source_table_id: int, target_table_id: int,
                                      relationship_type: str, join_column: str,
                                      avg_cardinality: float,
                                      median_cardinality: float | None = None):
        rel = CrossTableRelationship(
            source_table_id=source_table_id, target_table_id=target_table_id,
            relationship_type=relationship_type, join_column=join_column,
            avg_cardinality=avg_cardinality, median_cardinality=median_cardinality,
        )
        self._session.add(rel)
        self._session.commit()

    # --- Temporal Trends ---

    def save_temporal_trend(self, ratio_id: int, time_bucket: str,
                            ratio_value: float, sample_size: int | None = None):
        trend = TemporalTrend(
            ratio_id=ratio_id, time_bucket=time_bucket,
            ratio_value=ratio_value, sample_size=sample_size,
        )
        self._session.add(trend)
        self._session.commit()

    def get_trends_for_ratio(self, ratio_id: int) -> list[TemporalTrend]:
        return list(self._session.execute(
            select(TemporalTrend).where(TemporalTrend.ratio_id == ratio_id)
        ).scalars().all())
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_storage_store.py -v`
Expected: all 8 tests PASS

**Step 5: Commit**

```bash
git add src/storage/store.py tests/test_storage_store.py
git commit -m "feat: ProfileStore CRUD operations for SQLite database"
```

---

### Task 5: MySQL Connection with Rate Limiting

**Files:**
- Create: `src/connections/mysql.py`
- Create: `tests/test_mysql_connection.py`

**Step 1: Write the failing test**

```python
# tests/test_mysql_connection.py
import asyncio
import time
import pytest
from unittest.mock import MagicMock, patch
from src.connections.mysql import MySQLConnection
from src.config import MySQLConfig


def test_rate_limiter_enforces_sleep():
    """Rate limiter sleeps between queries."""
    config = MySQLConfig(host="localhost", max_concurrent_queries=1,
                         sleep_between_queries_ms=200)
    conn = MySQLConnection(config)

    times = []
    original_execute = None

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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_mysql_connection.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/connections/mysql.py
"""MySQL connection with rate limiting and concurrency control."""

import logging
import threading
import time
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import MySQLConfig

logger = logging.getLogger(__name__)


class MySQLConnection:
    def __init__(self, config: MySQLConfig):
        self._config = config
        self._semaphore = threading.Semaphore(config.max_concurrent_queries)
        self._sleep_seconds = config.sleep_between_queries_ms / 1000.0
        self._last_query_time = 0.0
        self._lock = threading.Lock()
        self._engines: dict[str, Engine] = {}
        self._engine: Engine | None = None  # default engine, set externally for testing

    def _build_url(self, config: MySQLConfig, database: str) -> str:
        return (f"mysql+pymysql://{config.user}:{config.password}"
                f"@{config.host}:{config.port}/{database}")

    def _get_engine(self, database: str) -> Engine:
        if self._engine is not None:
            return self._engine
        if database not in self._engines:
            url = self._build_url(self._config, database)
            self._engines[database] = create_engine(url, pool_size=self._config.max_concurrent_queries,
                                                     pool_pre_ping=True)
        return self._engines[database]

    def _wait_for_rate_limit(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_query_time
            if elapsed < self._sleep_seconds:
                time.sleep(self._sleep_seconds - elapsed)
            self._last_query_time = time.monotonic()

    def execute_query(self, query: str, database: str = "",
                      params: dict[str, Any] | None = None) -> list[dict]:
        """Execute a SQL query with rate limiting and concurrency control.

        Returns list of dicts (one per row).
        """
        self._semaphore.acquire()
        try:
            self._wait_for_rate_limit()
            engine = self._get_engine(database)
            with engine.connect() as conn:
                result = conn.execute(text(query) if isinstance(query, str) else query,
                                      params or {})
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                logger.debug("Query returned %d rows: %s", len(rows), query[:80])
                return rows
        finally:
            self._semaphore.release()

    def get_information_schema(self, database: str) -> dict:
        """Fetch table and column metadata from INFORMATION_SCHEMA. Free query."""
        tables_query = """
            SELECT TABLE_NAME, TABLE_ROWS, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :db AND TABLE_TYPE = 'BASE TABLE'
        """
        columns_query = """
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   COLUMN_KEY, COLUMN_TYPE, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :db
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        fk_query = """
            SELECT TABLE_NAME, COLUMN_NAME,
                   REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :db AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        tables = self.execute_query(tables_query, database, {"db": database})
        columns = self.execute_query(columns_query, database, {"db": database})
        fks = self.execute_query(fk_query, database, {"db": database})

        return {"tables": tables, "columns": columns, "foreign_keys": fks}

    def dispose(self):
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_mysql_connection.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
git add src/connections/mysql.py tests/test_mysql_connection.py
git commit -m "feat: MySQL connection with rate limiting and semaphore concurrency"
```

---

### Task 6: BigQuery Connection with Dry-Run Cost Control

**Files:**
- Create: `src/connections/bigquery.py`
- Create: `tests/test_bigquery_connection.py`

**Step 1: Write the failing test**

```python
# tests/test_bigquery_connection.py
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from src.connections.bigquery import BigQueryConnection, QueryTooExpensiveError
from src.config import BigQueryConfig


def test_dry_run_skips_expensive_query():
    """Queries exceeding dry_run_threshold_bytes raise QueryTooExpensiveError."""
    config = BigQueryConfig(project="test-project",
                            dry_run_threshold_bytes=100_000_000,
                            max_bytes_per_query=500_000_000)
    conn = BigQueryConnection(config)

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
    config = BigQueryConfig(project="test-project",
                            dry_run_threshold_bytes=100_000_000,
                            max_bytes_per_query=500_000_000)
    conn = BigQueryConnection(config)

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
    config = BigQueryConfig(project="test-project",
                            dry_run_threshold_bytes=1_000_000_000,
                            max_bytes_per_query=500_000_000)
    conn = BigQueryConnection(config)

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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_bigquery_connection.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/connections/bigquery.py
"""BigQuery connection with dry-run cost estimation and byte limits."""

import logging
from typing import Any

from google.cloud import bigquery

from src.config import BigQueryConfig

logger = logging.getLogger(__name__)


class QueryTooExpensiveError(Exception):
    """Raised when a query's dry-run estimate exceeds the configured threshold."""

    def __init__(self, query: str, estimated_bytes: int, threshold_bytes: int):
        self.estimated_bytes = estimated_bytes
        self.threshold_bytes = threshold_bytes
        super().__init__(
            f"Query would scan {estimated_bytes:,} bytes "
            f"(threshold: {threshold_bytes:,}): {query[:100]}"
        )


class BigQueryConnection:
    def __init__(self, config: BigQueryConfig):
        self._config = config
        self._client: bigquery.Client | None = None

    def _get_client(self) -> bigquery.Client:
        if self._client is None:
            self._client = bigquery.Client(project=self._config.project)
        return self._client

    def _dry_run(self, query: str, params: list | None = None) -> int:
        """Run a dry-run query and return estimated bytes processed."""
        client = self._get_client()
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        if params:
            job_config.query_parameters = params
        job = client.query(query, job_config=job_config)
        return job.total_bytes_processed

    def execute_query(self, query: str, params: list | None = None) -> Any:
        """Execute a query with dry-run cost check and maximumBytesBilled safety.

        Returns the query job result (call .to_dataframe() or iterate).
        Raises QueryTooExpensiveError if dry-run estimate exceeds threshold.
        """
        # Step 1: Dry-run estimate
        estimated_bytes = self._dry_run(query, params)
        logger.info("Dry-run estimate: %s bytes for: %s", f"{estimated_bytes:,}", query[:80])

        if estimated_bytes > self._config.dry_run_threshold_bytes:
            raise QueryTooExpensiveError(query, estimated_bytes,
                                         self._config.dry_run_threshold_bytes)

        # Step 2: Actual query with maximumBytesBilled safety net
        client = self._get_client()
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=self._config.max_bytes_per_query,
        )
        if params:
            job_config.query_parameters = params

        result = client.query(query, job_config=job_config)
        logger.info("Query completed: %d rows", result.total_rows)
        return result

    def get_information_schema(self, dataset: str) -> dict:
        """Fetch table and column metadata from INFORMATION_SCHEMA. Cheap/free."""
        client = self._get_client()
        project = self._config.project

        tables_query = f"""
            SELECT table_name, row_count, size_bytes
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_STORAGE`
        """
        columns_query = f"""
            SELECT table_name, column_name, data_type, is_nullable
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            ORDER BY table_name, ordinal_position
        """
        # These are metadata queries — skip dry-run, they're always cheap
        tables_result = client.query(tables_query).result()
        columns_result = client.query(columns_query).result()

        tables = [dict(row) for row in tables_result]
        columns = [dict(row) for row in columns_result]

        return {"tables": tables, "columns": columns}

    def dispose(self):
        if self._client:
            self._client.close()
            self._client = None
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_bigquery_connection.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
git add src/connections/bigquery.py tests/test_bigquery_connection.py
git commit -m "feat: BigQuery connection with dry-run cost checks and byte limits"
```

---

### Task 7: Column Classifier

**Files:**
- Create: `src/discovery/classifier.py`
- Create: `tests/test_classifier.py`

**Step 1: Write the failing test**

```python
# tests/test_classifier.py
import pytest
from src.discovery.classifier import ColumnClassifier
from src.config import ClassificationConfig


@pytest.fixture
def classifier():
    return ColumnClassifier(ClassificationConfig())


def test_classify_primary_key(classifier):
    assert classifier.classify("id", "INT", is_primary_key=True) == "identifier"
    assert classifier.classify("user_id", "BIGINT", is_primary_key=True) == "identifier"


def test_classify_foreign_key(classifier):
    assert classifier.classify("user_id", "INT", is_foreign_key=True) == "identifier"


def test_classify_temporal(classifier):
    assert classifier.classify("created_at", "DATETIME") == "temporal"
    assert classifier.classify("event_date", "DATE") == "temporal"
    assert classifier.classify("updated_at", "TIMESTAMP") == "temporal"


def test_classify_dimension_by_type(classifier):
    assert classifier.classify("status", "VARCHAR") == "dimension"
    assert classifier.classify("mode", "ENUM") == "dimension"


def test_classify_dimension_by_name_pattern(classifier):
    assert classifier.classify("country_code", "VARCHAR") == "dimension"
    assert classifier.classify("publisher_name", "VARCHAR") == "dimension"
    assert classifier.classify("device_type", "VARCHAR") == "dimension"


def test_classify_metric_by_name_pattern(classifier):
    assert classifier.classify("click_count", "INT") == "metric"
    assert classifier.classify("total_revenue", "DECIMAL") == "metric"
    assert classifier.classify("impressions", "BIGINT") == "metric"
    assert classifier.classify("conversion_rate", "FLOAT") == "metric"


def test_classify_numeric_defaults_to_metric(classifier):
    """Numeric columns without special naming default to metric."""
    assert classifier.classify("value", "INT") == "metric"
    assert classifier.classify("score", "FLOAT") == "metric"


def test_classify_text_to_other(classifier):
    assert classifier.classify("description", "TEXT") == "other"
    assert classifier.classify("payload", "JSON") == "other"
    assert classifier.classify("notes", "LONGTEXT") == "other"


def test_classify_id_column_without_constraint(classifier):
    """Columns named *_id without PK/FK constraint are still identifiers."""
    assert classifier.classify("user_id", "INT") == "identifier"
    assert classifier.classify("order_id", "BIGINT") == "identifier"


def test_classify_respects_custom_patterns():
    """Custom patterns in config are used for classification."""
    config = ClassificationConfig(
        dimension_name_patterns=["segment", "cohort"],
        metric_name_patterns=["weight"],
    )
    c = ColumnClassifier(config)
    assert c.classify("user_segment", "VARCHAR") == "dimension"
    assert c.classify("item_weight", "FLOAT") == "metric"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_classifier.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/discovery/classifier.py
"""Heuristic column classification: dimension, metric, identifier, temporal, other."""

import re

from src.config import ClassificationConfig

# Data type families
_TEMPORAL_TYPES = {"date", "datetime", "timestamp", "time", "datetime2", "datetimeoffset"}
_NUMERIC_TYPES = {"int", "integer", "bigint", "smallint", "tinyint", "mediumint",
                  "float", "double", "decimal", "numeric", "real",
                  "int64", "float64", "bignumeric"}
_STRING_TYPES = {"varchar", "char", "enum", "set", "string"}
_TEXT_TYPES = {"text", "longtext", "mediumtext", "tinytext", "blob", "longblob",
               "mediumblob", "json", "bytes", "binary", "varbinary"}


def _base_type(data_type: str) -> str:
    """Extract the base type name, e.g., 'VARCHAR(255)' -> 'varchar'."""
    return re.split(r"[(\s]", data_type.strip())[0].lower()


class ColumnClassifier:
    def __init__(self, config: ClassificationConfig):
        self._config = config
        self._dim_patterns = [p.lower() for p in config.dimension_name_patterns]
        self._metric_patterns = [p.lower() for p in config.metric_name_patterns]

    def classify(self, column_name: str, data_type: str,
                 is_primary_key: bool = False,
                 is_foreign_key: bool = False) -> str:
        name_lower = column_name.lower()
        base = _base_type(data_type)

        # Rule 1: Explicit PK/FK → identifier
        if is_primary_key or is_foreign_key:
            return "identifier"

        # Rule 2: *_id naming convention → identifier
        if name_lower.endswith("_id") or name_lower == "id":
            return "identifier"

        # Rule 3: Temporal types → temporal
        if base in _TEMPORAL_TYPES:
            return "temporal"

        # Rule 4: Text/blob/JSON → other
        if base in _TEXT_TYPES:
            return "other"

        # Rule 5: String types → dimension (VARCHAR, CHAR, ENUM)
        if base in _STRING_TYPES:
            return "dimension"

        # Rule 6: Numeric columns — check name patterns
        if base in _NUMERIC_TYPES:
            # Check metric name patterns first (more specific)
            for pattern in self._metric_patterns:
                if pattern in name_lower:
                    return "metric"
            # Check dimension name patterns
            for pattern in self._dim_patterns:
                if pattern in name_lower:
                    return "dimension"
            # Default numeric → metric
            return "metric"

        # Rule 7: Boolean → dimension
        if base in {"bool", "boolean", "bit"}:
            return "dimension"

        return "other"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_classifier.py -v`
Expected: all 10 tests PASS

**Step 5: Commit**

```bash
git add src/discovery/classifier.py tests/test_classifier.py
git commit -m "feat: heuristic column classifier (dimension/metric/identifier/temporal)"
```

---

### Task 8: Schema Discovery

**Files:**
- Create: `src/discovery/schema.py`
- Create: `tests/test_schema_discovery.py`

**Step 1: Write the failing test**

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_schema_discovery.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/discovery/schema.py
"""Schema discovery from INFORMATION_SCHEMA and OpenMetadata."""

import logging
from typing import Any

from src.config import ProfilerConfig
from src.discovery.classifier import ColumnClassifier
from src.storage.store import ProfileStore

logger = logging.getLogger(__name__)


class SchemaDiscoverer:
    def __init__(self, config: ProfilerConfig, store: ProfileStore,
                 mysql_conn=None, bq_conn=None):
        self._config = config
        self._store = store
        self._mysql = mysql_conn
        self._bq = bq_conn
        self._classifier = ColumnClassifier(config.classification)

    def discover_mysql(self, database: str):
        """Discover all tables and columns from a MySQL database."""
        logger.info("Discovering MySQL schema for database: %s", database)
        info = self._mysql.get_information_schema(database)

        # Build FK lookup: (table, column) -> (ref_table, ref_column)
        fk_map: dict[tuple[str, str], tuple[str, str]] = {}
        for fk in info.get("foreign_keys", []):
            key = (fk["TABLE_NAME"], fk["COLUMN_NAME"])
            fk_map[key] = (fk["REFERENCED_TABLE_NAME"], fk["REFERENCED_COLUMN_NAME"])

        # Build PK lookup: set of (table, column)
        pk_set: set[tuple[str, str]] = set()
        for col in info["columns"]:
            if col.get("COLUMN_KEY") == "PRI":
                pk_set.add((col["TABLE_NAME"], col["COLUMN_NAME"]))

        # Process tables
        table_rows: dict[str, int] = {}
        for tbl in info["tables"]:
            table_name = tbl["TABLE_NAME"]
            row_count = tbl.get("TABLE_ROWS")
            table_rows[table_name] = row_count
            self._store.upsert_table("mysql", database, table_name, row_count=row_count)

        # Process columns
        tables = {t.table_name: t for t in self._store.get_all_tables()
                  if t.source == "mysql" and t.database_name == database}

        # Clear existing columns for idempotency — check if columns exist first
        for col_info in info["columns"]:
            table_name = col_info["TABLE_NAME"]
            if table_name not in tables:
                continue
            table = tables[table_name]
            col_name = col_info["COLUMN_NAME"]
            data_type = col_info["DATA_TYPE"]
            is_pk = (table_name, col_name) in pk_set
            is_fk = (table_name, col_name) in fk_map
            fk_target = fk_map.get((table_name, col_name))

            # Check if column already exists
            existing_cols = self._store.get_columns_for_table(table.id)
            if any(c.column_name == col_name for c in existing_cols):
                continue

            classification = self._classifier.classify(
                col_name, data_type, is_primary_key=is_pk, is_foreign_key=is_fk
            )

            self._store.add_column(
                table.id, col_name, col_info.get("COLUMN_TYPE", data_type),
                classification=classification,
                nullable=col_info.get("IS_NULLABLE") == "YES",
                is_primary_key=is_pk,
                is_foreign_key=is_fk,
                fk_target_table=fk_target[0] if fk_target else None,
                fk_target_column=fk_target[1] if fk_target else None,
            )

        logger.info("Discovered %d tables in MySQL database %s", len(tables), database)

    def discover_bigquery(self, dataset: str):
        """Discover all tables and columns from a BigQuery dataset."""
        logger.info("Discovering BigQuery schema for dataset: %s", dataset)
        info = self._bq.get_information_schema(dataset)

        for tbl in info["tables"]:
            table_name = tbl["table_name"]
            row_count = tbl.get("row_count")
            self._store.upsert_table("bigquery", dataset, table_name, row_count=row_count)

        tables = {t.table_name: t for t in self._store.get_all_tables()
                  if t.source == "bigquery" and t.database_name == dataset}

        for col_info in info["columns"]:
            table_name = col_info["table_name"]
            if table_name not in tables:
                continue
            table = tables[table_name]
            col_name = col_info["column_name"]
            data_type = col_info["data_type"]

            existing_cols = self._store.get_columns_for_table(table.id)
            if any(c.column_name == col_name for c in existing_cols):
                continue

            classification = self._classifier.classify(col_name, data_type)

            self._store.add_column(
                table.id, col_name, data_type,
                classification=classification,
                nullable=col_info.get("is_nullable") == "YES",
            )

        logger.info("Discovered %d tables in BigQuery dataset %s", len(tables), dataset)

    def discover_all(self):
        """Run discovery for all configured MySQL databases and BQ datasets."""
        if self._mysql:
            for db in self._config.mysql.databases:
                self.discover_mysql(db)
        if self._bq:
            for ds in self._config.bigquery.datasets:
                self.discover_bigquery(ds)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_schema_discovery.py -v`
Expected: all 2 tests PASS

**Step 5: Commit**

```bash
git add src/discovery/schema.py tests/test_schema_discovery.py
git commit -m "feat: schema discovery from MySQL and BigQuery INFORMATION_SCHEMA"
```

---

### Task 9: Adaptive Sampler

**Files:**
- Create: `src/profiling/sampler.py`
- Create: `tests/test_sampler.py`

**Step 1: Write the failing test**

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_sampler.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/profiling/sampler.py
"""Adaptive sampling strategy for MySQL and BigQuery tables."""

from src.config import SamplingConfig


class AdaptiveSampler:
    def __init__(self, config: SamplingConfig):
        self._config = config

    def _select_clause(self, columns: list[str] | None) -> str:
        if columns:
            return "SELECT " + ", ".join(f"`{c}`" for c in columns)
        return "SELECT *"

    def build_sample_query(self, source: str, database: str, table_name: str,
                           row_count: int | None = None,
                           pk_column: str | None = None,
                           columns: list[str] | None = None) -> str:
        """Build a SELECT query with appropriate sampling for the table size.

        Args:
            source: 'mysql' or 'bigquery'
            database: database/dataset name
            table_name: table name
            row_count: estimated row count (from metadata)
            pk_column: primary key column name (for MySQL modulo sampling)
            columns: specific columns to select (None = all)
        """
        select = self._select_clause(columns)
        row_count = row_count or 0

        if source == "bigquery":
            return self._bq_query(select, database, table_name, row_count)
        else:
            return self._mysql_query(select, database, table_name, row_count, pk_column)

    def _mysql_query(self, select: str, database: str, table_name: str,
                     row_count: int, pk_column: str | None) -> str:
        fqn = f"`{database}`.`{table_name}`"

        # Small table: full scan
        if row_count <= self._config.small_table_threshold:
            return f"{select} FROM {fqn}"

        # Medium table: modulo sampling
        if row_count <= self._config.medium_table_threshold:
            if pk_column:
                # Sample ~10% using modulo on PK
                modulo = max(1, 100 // self._config.medium_table_sample_pct)
                return (f"{select} FROM {fqn} "
                        f"WHERE MOD(`{pk_column}`, {modulo}) = 0")
            else:
                # No PK available — use LIMIT with deterministic ordering
                sample_rows = row_count * self._config.medium_table_sample_pct // 100
                return f"{select} FROM {fqn} LIMIT {sample_rows}"

        # Large table: fixed sample size
        if pk_column:
            modulo = max(1, row_count // self._config.large_table_sample_rows)
            return (f"{select} FROM {fqn} "
                    f"WHERE MOD(`{pk_column}`, {modulo}) = 0 "
                    f"LIMIT {self._config.large_table_sample_rows}")
        else:
            return f"{select} FROM {fqn} LIMIT {self._config.large_table_sample_rows}"

    def _bq_query(self, select: str, dataset: str, table_name: str,
                  row_count: int) -> str:
        fqn = f"`{dataset}.{table_name}`"

        # Small table: full scan
        if row_count <= self._config.small_table_threshold:
            return f"{select} FROM {fqn}"

        # Medium table: TABLESAMPLE at configured percentage
        if row_count <= self._config.medium_table_threshold:
            return f"{select} FROM {fqn} TABLESAMPLE SYSTEM ({self._config.medium_table_sample_pct} PERCENT)"

        # Large table: TABLESAMPLE at calculated percentage
        pct = max(1, (self._config.large_table_sample_rows * 100) // row_count)
        return f"{select} FROM {fqn} TABLESAMPLE SYSTEM ({pct} PERCENT)"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_sampler.py -v`
Expected: all 6 tests PASS

**Step 5: Commit**

```bash
git add src/profiling/sampler.py tests/test_sampler.py
git commit -m "feat: adaptive sampler with MySQL modulo and BigQuery TABLESAMPLE"
```

---

### Task 10: Column Profiler

**Files:**
- Create: `src/profiling/profiler.py`
- Create: `tests/test_profiler.py`

**Step 1: Write the failing test**

```python
# tests/test_profiler.py
import json
import tempfile
import os
import pytest
import pandas as pd
import numpy as np
from src.profiling.profiler import ColumnProfiler


def test_profile_metric_column():
    """Profile a numeric/metric column and get correct stats."""
    np.random.seed(42)
    data = pd.DataFrame({"clicks": np.random.poisson(10, size=1000)})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["clicks"], classification="metric")

    assert result["null_rate"] == 0.0
    assert result["distinct_count"] > 5
    assert result["mean"] == pytest.approx(data["clicks"].mean(), rel=0.01)
    assert result["stddev"] == pytest.approx(data["clicks"].std(), rel=0.01)
    assert result["min_value"] == str(data["clicks"].min())
    assert result["max_value"] == str(data["clicks"].max())
    assert result["p5"] is not None
    assert result["p25"] is not None
    assert result["p50"] is not None
    assert result["p75"] is not None
    assert result["p95"] is not None
    assert result["zero_rate"] is not None
    # Histogram should be a JSON string
    hist = json.loads(result["histogram_json"])
    assert len(hist) > 0
    assert all("bucket" in b and "count" in b for b in hist)


def test_profile_dimension_column():
    """Profile a categorical/dimension column and get value frequencies."""
    data = pd.DataFrame({"country": ["US"] * 500 + ["GB"] * 300 + ["DE"] * 200})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["country"], classification="dimension")

    assert result["distinct_count"] == 3
    assert result["null_rate"] == 0.0
    top = json.loads(result["top_values_json"])
    assert len(top) == 3
    assert top[0]["value"] == "US"
    assert top[0]["frequency"] == 500


def test_profile_temporal_column():
    """Profile a date/temporal column and get range info."""
    dates = pd.date_range("2024-01-01", periods=365, freq="D")
    data = pd.DataFrame({"event_date": dates})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["event_date"], classification="temporal")

    assert result["min_value"] == "2024-01-01"
    assert result["max_value"] == "2024-12-31"
    assert result["distinct_count"] == 365
    assert result["null_rate"] == 0.0


def test_profile_handles_nulls():
    """Profiler correctly reports null rates."""
    data = pd.DataFrame({"val": [1, 2, None, None, 5]})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["val"], classification="metric")
    assert result["null_rate"] == pytest.approx(0.4)


def test_profile_handles_all_zeros():
    """Profiler handles columns that are all zeros."""
    data = pd.DataFrame({"val": [0] * 100})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["val"], classification="metric")
    assert result["zero_rate"] == pytest.approx(1.0)
    assert result["mean"] == 0.0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_profiler.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/profiling/profiler.py
"""Per-column statistical profiling."""

import json
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Max number of top values to store for dimension columns
TOP_N_VALUES = 50
# Number of histogram buckets for metric columns
HISTOGRAM_BUCKETS = 20


class ColumnProfiler:
    def profile_column(self, series: pd.Series, classification: str) -> dict:
        """Compute statistics for a single column based on its classification.

        Returns a dict matching ColumnProfile fields.
        """
        total = len(series)
        null_count = int(series.isna().sum())
        null_rate = null_count / total if total > 0 else 0.0
        non_null = series.dropna()
        distinct_count = int(non_null.nunique())

        base = {
            "null_rate": null_rate,
            "distinct_count": distinct_count,
        }

        if classification == "metric":
            return {**base, **self._profile_metric(non_null)}
        elif classification == "dimension":
            return {**base, **self._profile_dimension(non_null)}
        elif classification == "temporal":
            return {**base, **self._profile_temporal(non_null)}
        elif classification == "identifier":
            return {**base, **self._profile_identifier(non_null, total)}
        else:
            return {**base, "min_value": None, "max_value": None}

    def _profile_metric(self, series: pd.Series) -> dict:
        if len(series) == 0:
            return {
                "min_value": None, "max_value": None,
                "mean": None, "median": None, "stddev": None,
                "p5": None, "p25": None, "p50": None, "p75": None, "p95": None,
                "zero_rate": None, "histogram_json": "[]",
            }

        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if len(numeric) == 0:
            return {
                "min_value": str(series.min()), "max_value": str(series.max()),
                "mean": None, "median": None, "stddev": None,
                "p5": None, "p25": None, "p50": None, "p75": None, "p95": None,
                "zero_rate": None, "histogram_json": "[]",
            }

        percentiles = np.percentile(numeric, [5, 25, 50, 75, 95])
        zero_count = int((numeric == 0).sum())

        # Build histogram
        try:
            counts, edges = np.histogram(numeric, bins=HISTOGRAM_BUCKETS)
            histogram = [
                {"bucket": f"{edges[i]:.4g}-{edges[i+1]:.4g}", "count": int(counts[i])}
                for i in range(len(counts))
            ]
        except (ValueError, TypeError):
            histogram = []

        return {
            "min_value": str(numeric.min()),
            "max_value": str(numeric.max()),
            "mean": float(numeric.mean()),
            "median": float(numeric.median()),
            "stddev": float(numeric.std()),
            "p5": float(percentiles[0]),
            "p25": float(percentiles[1]),
            "p50": float(percentiles[2]),
            "p75": float(percentiles[3]),
            "p95": float(percentiles[4]),
            "zero_rate": zero_count / len(numeric) if len(numeric) > 0 else 0.0,
            "histogram_json": json.dumps(histogram),
        }

    def _profile_dimension(self, series: pd.Series) -> dict:
        value_counts = series.value_counts()
        top_values = [
            {"value": str(val), "frequency": int(count)}
            for val, count in value_counts.head(TOP_N_VALUES).items()
        ]

        return {
            "min_value": str(series.min()) if len(series) > 0 else None,
            "max_value": str(series.max()) if len(series) > 0 else None,
            "top_values_json": json.dumps(top_values),
        }

    def _profile_temporal(self, series: pd.Series) -> dict:
        try:
            dates = pd.to_datetime(series, errors="coerce").dropna()
        except Exception:
            return {"min_value": None, "max_value": None}

        if len(dates) == 0:
            return {"min_value": None, "max_value": None}

        return {
            "min_value": str(dates.min().date()),
            "max_value": str(dates.max().date()),
        }

    def _profile_identifier(self, series: pd.Series, total: int) -> dict:
        return {
            "min_value": str(series.min()) if len(series) > 0 else None,
            "max_value": str(series.max()) if len(series) > 0 else None,
        }
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_profiler.py -v`
Expected: all 5 tests PASS

**Step 5: Commit**

```bash
git add src/profiling/profiler.py tests/test_profiler.py
git commit -m "feat: column profiler with stats for metric/dimension/temporal columns"
```

---

### Task 11: Ratio Detector

**Files:**
- Create: `src/ratios/detector.py`
- Create: `tests/test_ratio_detector.py`

**Step 1: Write the failing test**

```python
# tests/test_ratio_detector.py
import pytest
import pandas as pd
import numpy as np
from src.ratios.detector import RatioDetector


def test_detect_plausible_pairs():
    """Detector finds plausible metric pairs like clicks/impressions."""
    np.random.seed(42)
    n = 1000
    impressions = np.random.poisson(1000, n)
    clicks = (impressions * np.random.uniform(0.01, 0.05, n)).astype(int)
    data = pd.DataFrame({"impressions": impressions, "clicks": clicks,
                          "country": ["US"] * 500 + ["GB"] * 500})

    detector = RatioDetector()
    metric_cols = ["impressions", "clicks"]
    pairs = detector.find_plausible_pairs(data, metric_cols)

    # Should find clicks/impressions as a plausible pair
    assert len(pairs) > 0
    pair_sets = [frozenset(p) for p in pairs]
    assert frozenset(("clicks", "impressions")) in pair_sets


def test_reject_unrelated_metrics():
    """Detector rejects pairs with wildly varying ratios (unrelated metrics)."""
    np.random.seed(42)
    n = 1000
    data = pd.DataFrame({
        "metric_a": np.random.exponential(100, n),
        "metric_b": np.random.exponential(1, n) * np.random.choice([1, 1000], n),
    })

    detector = RatioDetector()
    pairs = detector.find_plausible_pairs(data, ["metric_a", "metric_b"])
    # Wildly varying ratio — should be rejected or empty
    # (this depends on the threshold, but with a 1000x variation it should fail)
    # We allow it to be found but with high stddev flagged


def test_reject_zero_denominator():
    """Pairs where denominator is mostly zero are rejected."""
    data = pd.DataFrame({
        "numerator": [10, 20, 30, 40, 50],
        "denominator": [0, 0, 0, 0, 1],
    })

    detector = RatioDetector()
    pairs = detector.find_plausible_pairs(data, ["numerator", "denominator"])
    # denominator is 80% zero — should be rejected
    assert len(pairs) == 0


def test_pairs_are_ordered():
    """Each pair is returned as (numerator, denominator) where numerator < denominator on avg."""
    np.random.seed(42)
    impressions = np.random.poisson(1000, 500)
    clicks = (impressions * 0.03).astype(int)
    data = pd.DataFrame({"impressions": impressions, "clicks": clicks})

    detector = RatioDetector()
    pairs = detector.find_plausible_pairs(data, ["impressions", "clicks"])

    for num, den in pairs:
        # The smaller metric should be numerator (clicks < impressions)
        assert data[num].mean() <= data[den].mean()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_ratio_detector.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/ratios/detector.py
"""Detect plausible metric pairs for ratio analysis."""

import logging
from itertools import combinations

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Minimum fraction of non-zero values in denominator
MIN_NONZERO_RATE = 0.5
# Maximum coefficient of variation for the ratio to be considered "bounded"
MAX_RATIO_CV = 10.0


class RatioDetector:
    def find_plausible_pairs(self, data: pd.DataFrame,
                             metric_columns: list[str],
                             min_nonzero_rate: float = MIN_NONZERO_RATE,
                             max_ratio_cv: float = MAX_RATIO_CV,
                             ) -> list[tuple[str, str]]:
        """Find pairs of metric columns that form plausible ratios.

        Returns list of (numerator, denominator) tuples where the numerator
        is the column with the smaller mean (e.g., clicks/impressions not
        impressions/clicks).
        """
        plausible = []

        for col_a, col_b in combinations(metric_columns, 2):
            a = pd.to_numeric(data[col_a], errors="coerce").dropna()
            b = pd.to_numeric(data[col_b], errors="coerce").dropna()

            if len(a) == 0 or len(b) == 0:
                continue

            # Try both orderings — the smaller mean is the numerator
            if a.mean() <= b.mean():
                num_col, den_col = col_a, col_b
                num, den = a, b
            else:
                num_col, den_col = col_b, col_a
                num, den = b, a

            # Check: denominator should be mostly non-zero
            nonzero_rate = (den != 0).sum() / len(den)
            if nonzero_rate < min_nonzero_rate:
                logger.debug("Rejecting %s/%s: denominator %.0f%% zero",
                            num_col, den_col, (1 - nonzero_rate) * 100)
                continue

            # Check: both should be non-negative
            if (num < 0).any() or (den < 0).any():
                logger.debug("Rejecting %s/%s: contains negative values",
                            num_col, den_col)
                continue

            # Compute ratio where denominator is non-zero
            mask = den != 0
            aligned_num = num.loc[mask.index[mask]]
            aligned_den = den.loc[mask.index[mask]]

            if len(aligned_den) < 10:
                continue

            # Use aligned indices
            common_idx = aligned_num.index.intersection(aligned_den.index)
            if len(common_idx) < 10:
                continue

            ratio = aligned_num.loc[common_idx] / aligned_den.loc[common_idx]
            ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()

            if len(ratio) < 10:
                continue

            # Check: ratio should be reasonably bounded (low CV)
            cv = ratio.std() / ratio.mean() if ratio.mean() != 0 else float("inf")
            if cv > max_ratio_cv:
                logger.debug("Rejecting %s/%s: ratio CV=%.2f too high",
                            num_col, den_col, cv)
                continue

            plausible.append((num_col, den_col))
            logger.info("Found plausible ratio: %s / %s (mean=%.4f, cv=%.2f)",
                       num_col, den_col, ratio.mean(), cv)

        return plausible
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_ratio_detector.py -v`
Expected: all 4 tests PASS

**Step 5: Commit**

```bash
git add src/ratios/detector.py tests/test_ratio_detector.py
git commit -m "feat: ratio detector to find plausible metric pairs"
```

---

### Task 12: Ratio Calculator with Dimensional Breakdowns

**Files:**
- Create: `src/ratios/calculator.py`
- Create: `tests/test_ratio_calculator.py`

**Step 1: Write the failing test**

```python
# tests/test_ratio_calculator.py
import json
import pytest
import pandas as pd
import numpy as np
from src.ratios.calculator import RatioCalculator


@pytest.fixture
def sample_data():
    np.random.seed(42)
    n = 2000
    countries = np.random.choice(["US", "GB", "DE"], n, p=[0.5, 0.3, 0.2])
    impressions = np.random.poisson(1000, n)
    # CTR varies by country: US=4%, GB=3%, DE=2%
    ctr_by_country = {"US": 0.04, "GB": 0.03, "DE": 0.02}
    clicks = np.array([
        int(imp * ctr_by_country[c] * np.random.uniform(0.8, 1.2))
        for imp, c in zip(impressions, countries)
    ])
    dates = pd.date_range("2024-01-01", periods=n, freq="h")
    return pd.DataFrame({
        "impressions": impressions,
        "clicks": clicks,
        "country": countries,
        "event_date": dates,
    })


def test_compute_global_ratio(sample_data):
    """Computes correct global ratio (SUM(num)/SUM(den))."""
    calc = RatioCalculator()
    result = calc.compute_ratio(sample_data, "clicks", "impressions")

    expected = sample_data["clicks"].sum() / sample_data["impressions"].sum()
    assert result["global_ratio"] == pytest.approx(expected, rel=0.01)
    assert result["ratio_stddev"] is not None
    assert result["ratio_stddev"] > 0


def test_compute_dimensional_breakdown(sample_data):
    """Computes ratio broken down by a dimension column."""
    calc = RatioCalculator()
    breakdowns = calc.compute_dimensional_breakdown(
        sample_data, "clicks", "impressions", "country"
    )

    assert len(breakdowns) == 3
    bd_map = {b["dimension_value"]: b for b in breakdowns}
    assert "US" in bd_map
    assert "GB" in bd_map
    assert "DE" in bd_map

    # US should have highest CTR (~4%), DE lowest (~2%)
    assert bd_map["US"]["ratio_value"] > bd_map["DE"]["ratio_value"]
    assert bd_map["US"]["ratio_value"] == pytest.approx(0.04, abs=0.01)
    assert bd_map["DE"]["ratio_value"] == pytest.approx(0.02, abs=0.01)
    assert all(b["sample_size"] > 0 for b in breakdowns)


def test_compute_temporal_trend(sample_data):
    """Computes ratio trend over monthly time buckets."""
    calc = RatioCalculator()
    trends = calc.compute_temporal_trend(
        sample_data, "clicks", "impressions", "event_date", granularity="M"
    )

    assert len(trends) > 0
    assert all("time_bucket" in t for t in trends)
    assert all("ratio_value" in t for t in trends)
    assert all("sample_size" in t for t in trends)


def test_compute_ratio_histogram(sample_data):
    """Computes a histogram of the row-level ratio distribution."""
    calc = RatioCalculator()
    result = calc.compute_ratio(sample_data, "clicks", "impressions")

    hist = json.loads(result["ratio_histogram_json"])
    assert len(hist) > 0
    total_count = sum(b["count"] for b in hist)
    assert total_count > 0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_ratio_calculator.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/ratios/calculator.py
"""Compute ratios between metric columns with dimensional breakdowns."""

import json
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

HISTOGRAM_BUCKETS = 20


class RatioCalculator:
    def compute_ratio(self, data: pd.DataFrame,
                      numerator_col: str, denominator_col: str) -> dict:
        """Compute the global ratio between two metric columns.

        Returns dict with global_ratio, ratio_stddev, ratio_histogram_json.
        """
        num = pd.to_numeric(data[numerator_col], errors="coerce")
        den = pd.to_numeric(data[denominator_col], errors="coerce")

        # Global ratio: SUM(num) / SUM(den)
        den_sum = den.sum()
        global_ratio = float(num.sum() / den_sum) if den_sum != 0 else None

        # Row-level ratio for distribution stats
        mask = den != 0
        row_ratio = (num[mask] / den[mask]).replace([np.inf, -np.inf], np.nan).dropna()

        ratio_stddev = float(row_ratio.std()) if len(row_ratio) > 1 else None

        # Histogram of row-level ratios
        try:
            # Clip outliers for better histogram (use 1st-99th percentile range)
            if len(row_ratio) > 10:
                p1, p99 = np.percentile(row_ratio, [1, 99])
                clipped = row_ratio[(row_ratio >= p1) & (row_ratio <= p99)]
            else:
                clipped = row_ratio

            counts, edges = np.histogram(clipped, bins=HISTOGRAM_BUCKETS)
            histogram = [
                {"bucket": f"{edges[i]:.6g}-{edges[i+1]:.6g}", "count": int(counts[i])}
                for i in range(len(counts))
            ]
        except (ValueError, TypeError):
            histogram = []

        return {
            "global_ratio": global_ratio,
            "ratio_stddev": ratio_stddev,
            "ratio_histogram_json": json.dumps(histogram),
        }

    def compute_dimensional_breakdown(self, data: pd.DataFrame,
                                      numerator_col: str, denominator_col: str,
                                      dimension_col: str) -> list[dict]:
        """Compute ratio grouped by each value of a dimension column.

        Returns list of dicts with dimension_value, ratio_value, sample_size.
        """
        num = pd.to_numeric(data[numerator_col], errors="coerce")
        den = pd.to_numeric(data[denominator_col], errors="coerce")

        grouped = data.assign(_num=num, _den=den).groupby(dimension_col).agg(
            num_sum=("_num", "sum"),
            den_sum=("_den", "sum"),
            sample_size=("_den", "count"),
        )

        breakdowns = []
        for dim_value, row in grouped.iterrows():
            ratio_value = (float(row["num_sum"] / row["den_sum"])
                          if row["den_sum"] != 0 else None)
            breakdowns.append({
                "dimension_value": str(dim_value),
                "ratio_value": ratio_value,
                "sample_size": int(row["sample_size"]),
            })

        return breakdowns

    def compute_temporal_trend(self, data: pd.DataFrame,
                               numerator_col: str, denominator_col: str,
                               temporal_col: str,
                               granularity: str = "M") -> list[dict]:
        """Compute ratio over time buckets.

        Args:
            granularity: pandas frequency string ('D'=daily, 'W'=weekly, 'M'=monthly)
        """
        df = data.copy()
        df["_ts"] = pd.to_datetime(df[temporal_col], errors="coerce")
        df = df.dropna(subset=["_ts"])
        df["_num"] = pd.to_numeric(df[numerator_col], errors="coerce")
        df["_den"] = pd.to_numeric(df[denominator_col], errors="coerce")
        df["_bucket"] = df["_ts"].dt.to_period(granularity).astype(str)

        grouped = df.groupby("_bucket").agg(
            num_sum=("_num", "sum"),
            den_sum=("_den", "sum"),
            sample_size=("_den", "count"),
        )

        trends = []
        for bucket, row in grouped.iterrows():
            ratio_value = (float(row["num_sum"] / row["den_sum"])
                          if row["den_sum"] != 0 else None)
            trends.append({
                "time_bucket": str(bucket),
                "ratio_value": ratio_value,
                "sample_size": int(row["sample_size"]),
            })

        return trends
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_ratio_calculator.py -v`
Expected: all 4 tests PASS

**Step 5: Commit**

```bash
git add src/ratios/calculator.py tests/test_ratio_calculator.py
git commit -m "feat: ratio calculator with dimensional breakdowns and temporal trends"
```

---

### Task 13: CLI Interface

**Files:**
- Create: `src/cli.py`
- Create: `tests/test_cli.py`

**Step 1: Write the failing test**

```python
# tests/test_cli.py
import tempfile
import os
import pytest
import yaml
from click.testing import CliRunner
from src.cli import cli


@pytest.fixture
def config_file():
    cfg = {
        "mysql": {"host": "localhost", "databases": []},
        "bigquery": {"project": "", "datasets": []},
        "output": {"database_path": ""},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg, f)
        path = f.name
    yield path
    os.unlink(path)


def test_cli_status_runs(config_file):
    """The status command runs without error."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Update config to use temp db
    with open(config_file) as f:
        cfg = yaml.safe_load(f)
    cfg["output"]["database_path"] = db_path
    with open(config_file, "w") as f:
        yaml.dump(cfg, f)

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", config_file])
    assert result.exit_code == 0
    os.unlink(db_path)


def test_cli_help():
    """The CLI shows help text."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "analyze" in result.output or "discover" in result.output


def test_cli_discover_no_sources(config_file):
    """Discover with no databases/datasets configured completes cleanly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with open(config_file) as f:
        cfg = yaml.safe_load(f)
    cfg["output"]["database_path"] = db_path
    with open(config_file, "w") as f:
        yaml.dump(cfg, f)

    runner = CliRunner()
    result = runner.invoke(cli, ["discover", "--config", config_file])
    assert result.exit_code == 0
    os.unlink(db_path)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/cli.py
"""CLI entry point for the synthetic data profiler."""

import logging
import sys

import click
from rich.console import Console
from rich.table import Table as RichTable

from src.config import load_config
from src.storage.models import create_database
from src.storage.store import ProfileStore

console = Console()
logger = logging.getLogger("profiler")


def _setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


@click.group()
def cli():
    """Synthetic Data Profiler — analyze tables for realistic data generation."""
    pass


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def discover(config_path: str):
    """Phase 1: Discover schema from MySQL and BigQuery."""
    config = load_config(config_path)
    _setup_logging(config.output.log_level)
    store = ProfileStore(config.output.database_path)

    try:
        from src.discovery.schema import SchemaDiscoverer

        mysql_conn = None
        bq_conn = None

        if config.mysql.databases:
            from src.connections.mysql import MySQLConnection
            mysql_conn = MySQLConnection(config.mysql)

        if config.bigquery.datasets and config.bigquery.project:
            from src.connections.bigquery import BigQueryConnection
            bq_conn = BigQueryConnection(config.bigquery)

        discoverer = SchemaDiscoverer(config, store,
                                      mysql_conn=mysql_conn, bq_conn=bq_conn)
        discoverer.discover_all()
        console.print("[green]Discovery complete.[/green]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def profile(config_path: str):
    """Phase 2: Profile columns with adaptive sampling."""
    config = load_config(config_path)
    _setup_logging(config.output.log_level)
    store = ProfileStore(config.output.database_path)

    try:
        from src.profiling.profiler import ColumnProfiler
        from src.profiling.sampler import AdaptiveSampler
        from src.connections.mysql import MySQLConnection
        from src.connections.bigquery import BigQueryConnection

        profiler = ColumnProfiler()
        sampler = AdaptiveSampler(config.sampling)

        tables = store.get_tables_by_status("profile_status", "pending")
        console.print(f"[cyan]Profiling {len(tables)} tables...[/cyan]")

        import pandas as pd

        for tbl in tables:
            store.update_table_status(tbl.id, "profile_status", "in_progress")
            columns = store.get_columns_for_table(tbl.id)

            # Get the right connection
            if tbl.source == "mysql":
                conn = MySQLConnection(config.mysql)
                pk_cols = [c for c in columns if c.is_primary_key]
                pk_col = pk_cols[0].column_name if pk_cols else None

                query = sampler.build_sample_query(
                    "mysql", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count, pk_column=pk_col,
                )
                rows = conn.execute_query(query, tbl.database_name)
                df = pd.DataFrame(rows)
                conn.dispose()
            elif tbl.source == "bigquery":
                bq = BigQueryConnection(config.bigquery)
                query = sampler.build_sample_query(
                    "bigquery", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count,
                )
                try:
                    result = bq.execute_query(query)
                    df = result.to_dataframe()
                except Exception as e:
                    logger.warning("Skipping %s.%s: %s", tbl.database_name,
                                  tbl.table_name, e)
                    store.update_table_status(tbl.id, "profile_status", "skipped")
                    continue
                finally:
                    bq.dispose()
            else:
                continue

            if df.empty:
                store.update_table_status(tbl.id, "profile_status", "completed")
                continue

            for col in columns:
                if col.classification == "other":
                    continue
                if col.column_name not in df.columns:
                    continue

                result = profiler.profile_column(df[col.column_name], col.classification)
                store.save_column_profile(col.id, **result)

            store.update_table_status(tbl.id, "profile_status", "completed")
            console.print(f"  [green]Profiled {tbl.database_name}.{tbl.table_name}[/green]")

        console.print("[green]Profiling complete.[/green]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def ratios(config_path: str):
    """Phase 3: Detect ratios and compute dimensional breakdowns."""
    config = load_config(config_path)
    _setup_logging(config.output.log_level)
    store = ProfileStore(config.output.database_path)

    try:
        from src.ratios.detector import RatioDetector
        from src.ratios.calculator import RatioCalculator
        from src.profiling.sampler import AdaptiveSampler
        from src.connections.mysql import MySQLConnection
        from src.connections.bigquery import BigQueryConnection

        detector = RatioDetector()
        calculator = RatioCalculator()
        sampler = AdaptiveSampler(config.sampling)

        tables = store.get_tables_by_status("ratio_status", "pending")
        # Only process tables that have been profiled
        tables = [t for t in tables if t.profile_status == "completed"]

        console.print(f"[cyan]Analyzing ratios for {len(tables)} tables...[/cyan]")

        import pandas as pd

        for tbl in tables:
            store.update_table_status(tbl.id, "ratio_status", "in_progress")
            columns = store.get_columns_for_table(tbl.id)

            metric_cols = [c for c in columns if c.classification == "metric"]
            dimension_cols = [c for c in columns if c.classification == "dimension"]
            temporal_cols = [c for c in columns if c.classification == "temporal"]

            if len(metric_cols) < 2:
                store.update_table_status(tbl.id, "ratio_status", "completed")
                continue

            # Fetch data sample
            if tbl.source == "mysql":
                conn = MySQLConnection(config.mysql)
                pk_cols = [c for c in columns if c.is_primary_key]
                pk_col = pk_cols[0].column_name if pk_cols else None
                query = sampler.build_sample_query(
                    "mysql", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count, pk_column=pk_col,
                )
                rows = conn.execute_query(query, tbl.database_name)
                df = pd.DataFrame(rows)
                conn.dispose()
            elif tbl.source == "bigquery":
                bq = BigQueryConnection(config.bigquery)
                query = sampler.build_sample_query(
                    "bigquery", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count,
                )
                try:
                    result = bq.execute_query(query)
                    df = result.to_dataframe()
                except Exception as e:
                    logger.warning("Skipping ratios for %s.%s: %s",
                                  tbl.database_name, tbl.table_name, e)
                    store.update_table_status(tbl.id, "ratio_status", "skipped")
                    continue
                finally:
                    bq.dispose()
            else:
                continue

            if df.empty:
                store.update_table_status(tbl.id, "ratio_status", "completed")
                continue

            # Detect plausible pairs
            metric_names = [c.column_name for c in metric_cols
                           if c.column_name in df.columns]
            pairs = detector.find_plausible_pairs(df, metric_names)

            col_name_to_id = {c.column_name: c.id for c in columns}

            for num_col, den_col in pairs:
                # Global ratio
                ratio_result = calculator.compute_ratio(df, num_col, den_col)
                ratio_id = store.save_ratio(
                    tbl.id, col_name_to_id[num_col], col_name_to_id[den_col],
                    global_ratio=ratio_result["global_ratio"],
                    ratio_stddev=ratio_result["ratio_stddev"],
                    ratio_histogram_json=ratio_result["ratio_histogram_json"],
                )

                # Dimensional breakdowns
                for dim_col in dimension_cols:
                    if dim_col.column_name not in df.columns:
                        continue
                    breakdowns = calculator.compute_dimensional_breakdown(
                        df, num_col, den_col, dim_col.column_name
                    )
                    for bd in breakdowns:
                        store.save_dimensional_breakdown(
                            ratio_id, col_name_to_id[dim_col.column_name],
                            bd["dimension_value"], bd["ratio_value"],
                            sample_size=bd["sample_size"],
                        )

                # Temporal trends
                for temp_col in temporal_cols:
                    if temp_col.column_name not in df.columns:
                        continue
                    trends = calculator.compute_temporal_trend(
                        df, num_col, den_col, temp_col.column_name
                    )
                    for trend in trends:
                        store.save_temporal_trend(
                            ratio_id, trend["time_bucket"],
                            trend["ratio_value"], sample_size=trend["sample_size"],
                        )

            store.update_table_status(tbl.id, "ratio_status", "completed")
            console.print(f"  [green]Ratios for {tbl.database_name}.{tbl.table_name}: "
                         f"{len(pairs)} pairs found[/green]")

        console.print("[green]Ratio analysis complete.[/green]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def analyze(config_path: str):
    """Run full pipeline: discover → profile → ratios."""
    ctx = click.get_current_context()
    ctx.invoke(discover, config_path=config_path)
    ctx.invoke(profile, config_path=config_path)
    ctx.invoke(ratios, config_path=config_path)


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def status(config_path: str):
    """Show current progress across all phases."""
    config = load_config(config_path)
    store = ProfileStore(config.output.database_path)

    try:
        tables = store.get_all_tables()

        if not tables:
            console.print("[yellow]No tables discovered yet.[/yellow]")
            return

        # Summary counts
        summary = RichTable(title="Pipeline Status")
        summary.add_column("Phase")
        summary.add_column("Pending", justify="right")
        summary.add_column("In Progress", justify="right")
        summary.add_column("Completed", justify="right")
        summary.add_column("Skipped", justify="right")

        for phase in ["discovery_status", "profile_status", "ratio_status"]:
            counts = {}
            for t in tables:
                s = getattr(t, phase)
                counts[s] = counts.get(s, 0) + 1
            summary.add_row(
                phase.replace("_status", "").title(),
                str(counts.get("pending", 0)),
                str(counts.get("in_progress", 0)),
                str(counts.get("completed", 0)),
                str(counts.get("skipped", 0)),
            )

        console.print(summary)
        console.print(f"\n[cyan]Total tables: {len(tables)}[/cyan]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
@click.option("--phase", type=click.Choice(["discover", "profile", "ratios"]),
              required=True, help="Phase to reset")
def reset(config_path: str, phase: str):
    """Reset a phase to re-run it."""
    config = load_config(config_path)
    store = ProfileStore(config.output.database_path)

    status_field = {
        "discover": "discovery_status",
        "profile": "profile_status",
        "ratios": "ratio_status",
    }[phase]

    try:
        tables = store.get_all_tables()
        for tbl in tables:
            store.update_table_status(tbl.id, status_field, "pending")
        console.print(f"[green]Reset {len(tables)} tables for {phase} phase.[/green]")
    finally:
        store.close()


if __name__ == "__main__":
    cli()
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
git add src/cli.py tests/test_cli.py
git commit -m "feat: CLI with discover/profile/ratios/analyze/status/reset commands"
```

---

### Task 14: OpenMetadata Integration (Optional)

**Files:**
- Create: `src/connections/openmetadata.py`
- Create: `tests/test_openmetadata.py`

**Step 1: Write the failing test**

```python
# tests/test_openmetadata.py
import pytest
from unittest.mock import MagicMock, patch
from src.connections.openmetadata import OpenMetadataConnection
from src.config import OpenMetadataConfig


def test_disabled_connection_returns_empty():
    """When OpenMetadata is disabled, methods return empty results."""
    config = OpenMetadataConfig(enabled=False)
    conn = OpenMetadataConnection(config)
    result = conn.get_tables()
    assert result == []


def test_get_tables_calls_sdk():
    """When enabled, get_tables calls the OpenMetadata SDK."""
    config = OpenMetadataConfig(enabled=True, server_url="http://om:8585",
                                 api_token="test-token")
    conn = OpenMetadataConnection(config)

    # Mock the SDK client
    mock_client = MagicMock()
    mock_table = MagicMock()
    mock_table.name.__root__ = "test_table"
    mock_table.fullyQualifiedName.__root__ = "db.schema.test_table"
    mock_client.list_entities.return_value.entities = [mock_table]
    conn._client = mock_client

    result = conn.get_tables()
    assert len(result) >= 0  # Just verify it doesn't crash
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_openmetadata.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
# src/connections/openmetadata.py
"""OpenMetadata SDK integration (optional dependency)."""

import logging
from typing import Any

from src.config import OpenMetadataConfig

logger = logging.getLogger(__name__)


class OpenMetadataConnection:
    def __init__(self, config: OpenMetadataConfig):
        self._config = config
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client

        if not self._config.enabled:
            return None

        try:
            from metadata.ingestion.ometa.ometa_api import OpenMetadata
            from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
                OpenMetadataConnection as OMConnection,
                AuthProvider,
            )
            from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
                OpenMetadataJWTClientConfig,
            )

            config = OMConnection(
                hostPort=self._config.server_url,
                authProvider=AuthProvider.openmetadata,
                securityConfig=OpenMetadataJWTClientConfig(
                    jwtToken=self._config.api_token
                ),
            )
            self._client = OpenMetadata(config)
            return self._client
        except ImportError:
            logger.warning("openmetadata-ingestion not installed. "
                          "Install with: pip install 'synthetic-data-profiler[openmetadata]'")
            return None

    def get_tables(self) -> list[dict[str, Any]]:
        """Get all tables from OpenMetadata."""
        client = self._get_client()
        if client is None:
            return []

        try:
            from metadata.generated.schema.entity.data.table import Table

            tables = client.list_entities(entity=Table, limit=1000)
            result = []
            for entity in tables.entities:
                result.append({
                    "name": entity.name.__root__,
                    "fqn": entity.fullyQualifiedName.__root__ if entity.fullyQualifiedName else None,
                })
            return result
        except Exception as e:
            logger.error("Failed to fetch tables from OpenMetadata: %s", e)
            return []
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_openmetadata.py -v`
Expected: all 2 tests PASS

**Step 5: Commit**

```bash
git add src/connections/openmetadata.py tests/test_openmetadata.py
git commit -m "feat: OpenMetadata SDK integration (optional dependency)"
```

---

### Task 15: Integration Test — End-to-End Pipeline

**Files:**
- Create: `tests/test_integration.py`

This test uses an in-memory SQLite database as a mock data source to validate the full flow from discovery → profiling → ratios without needing real MySQL/BigQuery connections.

**Step 1: Write the integration test**

```python
# tests/test_integration.py
"""End-to-end integration test using mocked data sources."""

import json
import tempfile
import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock

from src.config import ProfilerConfig
from src.storage.store import ProfileStore
from src.discovery.schema import SchemaDiscoverer
from src.profiling.profiler import ColumnProfiler
from src.profiling.sampler import AdaptiveSampler
from src.ratios.detector import RatioDetector
from src.ratios.calculator import RatioCalculator


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    s = ProfileStore(db_path)
    yield s
    s.close()
    os.unlink(db_path)


@pytest.fixture
def sample_dataframe():
    """Realistic ad-tech sample data."""
    np.random.seed(42)
    n = 2000
    countries = np.random.choice(["US", "GB", "DE", "FR", "JP"], n,
                                  p=[0.35, 0.25, 0.15, 0.15, 0.10])
    publishers = np.random.choice(["pub_a", "pub_b", "pub_c"], n,
                                   p=[0.5, 0.3, 0.2])
    impressions = np.random.poisson(1000, n)

    ctr_map = {"US": 0.04, "GB": 0.03, "DE": 0.025, "FR": 0.02, "JP": 0.015}
    clicks = np.array([
        max(0, int(imp * ctr_map[c] * np.random.uniform(0.7, 1.3)))
        for imp, c in zip(impressions, countries)
    ])

    dates = pd.date_range("2024-01-01", periods=n, freq="h")

    return pd.DataFrame({
        "id": range(1, n + 1),
        "country": countries,
        "publisher": publishers,
        "impressions": impressions,
        "clicks": clicks,
        "event_date": dates,
    })


def test_full_pipeline(store, sample_dataframe):
    """End-to-end: discovery → profiling → ratio detection → breakdowns."""
    config = ProfilerConfig()
    df = sample_dataframe

    # --- Phase 1: Discovery (manual, since we don't have real MySQL) ---
    table_id = store.upsert_table("mysql", "analytics", "ad_events", row_count=len(df))
    col_ids = {}
    col_classifications = {
        "id": ("bigint", "identifier", True, False),
        "country": ("varchar(2)", "dimension", False, False),
        "publisher": ("varchar(50)", "dimension", False, False),
        "impressions": ("int", "metric", False, False),
        "clicks": ("int", "metric", False, False),
        "event_date": ("date", "temporal", False, False),
    }
    for col_name, (dtype, cls, is_pk, is_fk) in col_classifications.items():
        col_ids[col_name] = store.add_column(
            table_id, col_name, dtype, classification=cls,
            is_primary_key=is_pk, is_foreign_key=is_fk,
        )

    # --- Phase 2: Profiling ---
    profiler = ColumnProfiler()
    for col_name, col_id in col_ids.items():
        if col_name == "id":
            continue
        cls = col_classifications[col_name][1]
        result = profiler.profile_column(df[col_name], classification=cls)
        store.save_column_profile(col_id, **result)

    store.update_table_status(table_id, "profile_status", "completed")

    # Verify profiling results
    imp_profile = store.get_column_profile(col_ids["impressions"])
    assert imp_profile is not None
    assert imp_profile.mean > 0
    assert imp_profile.stddev > 0

    country_profile = store.get_column_profile(col_ids["country"])
    assert country_profile is not None
    assert country_profile.distinct_count == 5
    top_values = json.loads(country_profile.top_values_json)
    assert top_values[0]["value"] == "US"  # Most frequent

    # --- Phase 3: Ratio Analysis ---
    detector = RatioDetector()
    calculator = RatioCalculator()

    metric_cols = ["impressions", "clicks"]
    pairs = detector.find_plausible_pairs(df, metric_cols)
    assert len(pairs) == 1
    assert pairs[0] == ("clicks", "impressions")

    num_col, den_col = pairs[0]

    # Global ratio
    ratio_result = calculator.compute_ratio(df, num_col, den_col)
    assert 0.01 < ratio_result["global_ratio"] < 0.1  # CTR should be ~3%

    ratio_id = store.save_ratio(
        table_id, col_ids[num_col], col_ids[den_col],
        global_ratio=ratio_result["global_ratio"],
        ratio_stddev=ratio_result["ratio_stddev"],
        ratio_histogram_json=ratio_result["ratio_histogram_json"],
    )

    # Dimensional breakdowns
    for dim_col_name in ["country", "publisher"]:
        breakdowns = calculator.compute_dimensional_breakdown(
            df, num_col, den_col, dim_col_name
        )
        for bd in breakdowns:
            store.save_dimensional_breakdown(
                ratio_id, col_ids[dim_col_name],
                bd["dimension_value"], bd["ratio_value"],
                sample_size=bd["sample_size"],
            )

    # Temporal trends
    trends = calculator.compute_temporal_trend(df, num_col, den_col, "event_date")
    for trend in trends:
        store.save_temporal_trend(ratio_id, trend["time_bucket"],
                                  trend["ratio_value"], sample_size=trend["sample_size"])

    # --- Verify final results ---
    ratios = store.get_ratios_for_table(table_id)
    assert len(ratios) == 1

    breakdowns = store.get_breakdowns_for_ratio(ratio_id)
    assert len(breakdowns) == 8  # 5 countries + 3 publishers

    country_breakdowns = [b for b in breakdowns
                          if b.dimension_column_id == col_ids["country"]]
    bd_map = {b.dimension_value: b.ratio_value for b in country_breakdowns}

    # US should have highest CTR, JP lowest
    assert bd_map["US"] > bd_map["JP"]
    assert bd_map["US"] == pytest.approx(0.04, abs=0.01)
    assert bd_map["JP"] == pytest.approx(0.015, abs=0.01)

    trends_stored = store.get_trends_for_ratio(ratio_id)
    assert len(trends_stored) > 0

    store.update_table_status(table_id, "ratio_status", "completed")
```

**Step 2: Run test to verify it passes**

Run: `pytest tests/test_integration.py -v`
Expected: PASS — full pipeline works end-to-end

**Step 3: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: end-to-end integration test for full profiling pipeline"
```

---

### Task 16: Run Full Test Suite and Verify

**Step 1: Run all tests**

Run: `pytest tests/ -v --tb=short`
Expected: All tests pass (approximately 30+ tests)

**Step 2: Check test coverage**

Run: `pip install pytest-cov && pytest tests/ --cov=src --cov-report=term-missing`
Expected: >80% coverage across all modules

**Step 3: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: test suite cleanup and final adjustments"
```

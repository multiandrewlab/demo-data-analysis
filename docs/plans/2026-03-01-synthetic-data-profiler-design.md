# Synthetic Data Profiler — Design Document

**Date:** 2026-03-01
**Status:** Approved
**Goal:** Analyze 600+ tables across MySQL and BigQuery to discover statistical patterns, ratios, and distributions that drive realistic synthetic data generation.

## Context

We have ~600 tables spread across MySQL and BigQuery. Before generating synthetic data, we need to understand the statistical properties of the real data — not just averages, but how metrics like CTR vary by dimensions like country and publisher. OpenMetadata is being set up in parallel to provide schema/relationship metadata; this system will integrate with it via the SDK when available.

## Architecture: Phased Pipeline

Four sequential phases, each independently runnable and resumable, with all results stored incrementally in a SQLite database.

```
Discovery → Profiling → Ratio Analysis → Report
    ↓           ↓            ↓              ↓
  SQLite     SQLite       SQLite         SQLite
```

### Phase 1: Discovery

**Purpose:** Catalog all tables, columns, types, and relationships without querying any data.

**Sources:**
- OpenMetadata SDK (when available) — table list, column metadata, tags, lineage
- INFORMATION_SCHEMA (always available) — COLUMNS, TABLES, KEY_COLUMN_USAGE, TABLE_CONSTRAINTS for MySQL; INFORMATION_SCHEMA.COLUMNS, TABLE_STORAGE for BigQuery

**Column classification heuristics:**
- **Dimension:** VARCHAR/ENUM type, low cardinality (< 1000 unique), name patterns (country, region, publisher, platform, status, type)
- **Metric:** INT/FLOAT/DECIMAL type, high cardinality, name patterns (count, amount, impressions, clicks, revenue, *_rate)
- **Identifier:** primary/foreign key constraints, *_id naming, unique index
- **Temporal:** DATE/DATETIME/TIMESTAMP type
- **Other:** TEXT blobs, JSON, etc. — skipped for ratio analysis

Row counts fetched from metadata (TABLE_ROWS / BQ storage metadata) — no data queries needed.

### Phase 2: Profiling

**Purpose:** Compute per-column statistical profiles by querying actual data.

**Adaptive sampling:**
- Small tables (< 100K rows): full table scan
- Medium tables (100K–10M rows): 10% sample (TABLESAMPLE for BQ, modulo-PK for MySQL)
- Large tables (> 10M rows): fixed 100K row sample

**Per-column statistics:**

| Column Type | Statistics |
|---|---|
| Metric | min, max, mean, median, stddev, percentiles (p5/p25/p50/p75/p95), null rate, zero rate, histogram |
| Dimension | distinct count, top-N value frequencies, null rate, value distribution |
| Temporal | min/max date, range, granularity, null rate, gaps |
| Identifier | distinct count, null rate, uniqueness ratio |

**Cost controls:**
- BigQuery: dry-run every query first; skip if estimated bytes > 500MB (configurable); set `maximumBytesBilled` on every job
- MySQL: semaphore limits concurrent queries to 2 (configurable); configurable sleep between queries (default 500ms)

**Resumability:** each table tracked as pending/in_progress/completed/skipped in SQLite.

### Phase 3: Ratio & Relationship Analysis

**Purpose:** Discover how metrics relate to each other and how those relationships vary by dimension.

**Intra-table ratios:**
1. Find all pairs of metric columns in each table
2. Filter to plausible pairs (both non-negative, denominator mostly non-zero, bounded ratio distribution)
3. For each pair compute:
   - Global ratio: SUM(a)/SUM(b) and AVG(a/b)
   - Dimensional breakdowns: ratio grouped by each dimension column (e.g., CTR by country)
   - Ratio distribution: histogram buckets
   - Temporal trends: ratio over time buckets (if temporal column present)

**Cross-table relationships:**
1. Use FK/join keys to identify related tables
2. Compute cardinality ratios (e.g., avg 3.2 orders per user)
3. For shared dimensions, compare metric distributions across tables

### Phase 4: Report

Summary of all discovered patterns, anomalies flagged, final SQLite database ready for synthetic data generation.

## SQLite Storage Schema

```sql
CREATE TABLE tables (
    id INTEGER PRIMARY KEY,
    source TEXT,
    database_name TEXT,
    table_name TEXT,
    row_count INTEGER,
    discovery_status TEXT,
    profile_status TEXT,
    ratio_status TEXT
);

CREATE TABLE columns (
    id INTEGER PRIMARY KEY,
    table_id INTEGER REFERENCES tables(id),
    column_name TEXT,
    data_type TEXT,
    classification TEXT,
    nullable BOOLEAN,
    is_primary_key BOOLEAN,
    is_foreign_key BOOLEAN,
    fk_target_table TEXT,
    fk_target_column TEXT
);

CREATE TABLE column_profiles (
    column_id INTEGER REFERENCES columns(id),
    null_rate REAL,
    distinct_count INTEGER,
    min_value TEXT,
    max_value TEXT,
    mean REAL,
    median REAL,
    stddev REAL,
    p5 REAL, p25 REAL, p75 REAL, p95 REAL,
    zero_rate REAL,
    histogram_json TEXT,
    top_values_json TEXT
);

CREATE TABLE ratios (
    id INTEGER PRIMARY KEY,
    table_id INTEGER REFERENCES tables(id),
    numerator_column_id INTEGER REFERENCES columns(id),
    denominator_column_id INTEGER REFERENCES columns(id),
    global_ratio REAL,
    ratio_stddev REAL,
    ratio_histogram_json TEXT
);

CREATE TABLE dimensional_breakdowns (
    ratio_id INTEGER REFERENCES ratios(id),
    dimension_column_id INTEGER REFERENCES columns(id),
    dimension_value TEXT,
    ratio_value REAL,
    sample_size INTEGER
);

CREATE TABLE cross_table_relationships (
    source_table_id INTEGER REFERENCES tables(id),
    target_table_id INTEGER REFERENCES tables(id),
    relationship_type TEXT,
    join_column TEXT,
    avg_cardinality REAL,
    median_cardinality REAL
);

CREATE TABLE temporal_trends (
    ratio_id INTEGER REFERENCES ratios(id),
    time_bucket TEXT,
    ratio_value REAL,
    sample_size INTEGER
);
```

## CLI Interface

```bash
# Full pipeline
python -m src.cli analyze --config config/settings.yaml

# Individual phases
python -m src.cli discover --config config/settings.yaml
python -m src.cli profile --config config/settings.yaml
python -m src.cli ratios --config config/settings.yaml

# Utilities
python -m src.cli status
python -m src.cli reset --phase profile
python -m src.cli export --format json
```

## Project Structure

```
demo-data-analysis/
├── config/
│   └── settings.yaml
├── src/
│   ├── __init__.py
│   ├── cli.py
│   ├── config.py
│   ├── connections/
│   │   ├── mysql.py
│   │   ├── bigquery.py
│   │   └── openmetadata.py
│   ├── discovery/
│   │   ├── schema.py
│   │   └── classifier.py
│   ├── profiling/
│   │   ├── profiler.py
│   │   └── sampler.py
│   ├── ratios/
│   │   ├── detector.py
│   │   └── calculator.py
│   └── storage/
│       ├── models.py
│       └── store.py
├── tests/
├── pyproject.toml
└── README.md
```

## Key Dependencies

- `click` — CLI framework
- `sqlalchemy` — MySQL connections and SQLite ORM
- `google-cloud-bigquery` — BigQuery client
- `pandas`, `numpy` — statistical computation
- `openmetadata-ingestion` — OpenMetadata SDK (optional)
- `pyyaml` — configuration
- `rich` — progress bars and status output

## Cost & Load Controls Summary

| Control | MySQL | BigQuery |
|---|---|---|
| Concurrency | Semaphore (default: 2) | N/A (API handles it) |
| Rate limiting | Sleep between queries (500ms) | N/A |
| Query size | LIMIT clauses, PK-modulo sampling | Dry-run estimates, skip > 500MB |
| Hard limit | N/A | maximumBytesBilled per query |
| Sampling | Modulo on PK, ORDER BY with seed | TABLESAMPLE SYSTEM |

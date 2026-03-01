# Synthetic Data Profiler

Analyze production databases (MySQL and BigQuery) to discover statistical patterns for realistic synthetic data generation. Profiles 600+ tables, classifies columns, computes distributions, and detects ratio relationships between metrics.

## Pipeline

The profiler runs a 3-phase pipeline, each phase resumable independently:

1. **Discover** — Catalog tables and columns from `INFORMATION_SCHEMA`, classify each column as dimension, metric, identifier, temporal, or other
2. **Profile** — Compute per-column statistics (null rate, distinct count, percentiles, histograms, top values) using adaptive sampling
3. **Ratios** — Detect plausible metric pairs (e.g. clicks/impressions), compute global ratios, dimensional breakdowns, and temporal trends

All results are stored incrementally in a local SQLite database.

## Setup

Requires Python 3.11+.

```bash
pip install -e .
```

For OpenMetadata integration:

```bash
pip install -e ".[openmetadata]"
```

## Configuration

Copy and edit the example config:

```bash
cp config/settings.yaml config/my-settings.yaml
```

Key sections:

| Section | Purpose |
|---------|---------|
| `mysql` | Host, credentials, list of databases to scan |
| `bigquery` | Projects and datasets, cost controls (max bytes, dry-run threshold) |
| `sampling` | Thresholds for small/medium/large tables, sample sizes |
| `classification` | Name patterns and cardinality limits for column classification |
| `output` | SQLite database path and log level |

## Usage

Run the full pipeline:

```bash
profiler analyze --config config/my-settings.yaml
```

Or run phases individually:

```bash
profiler discover --config config/my-settings.yaml
profiler profile --config config/my-settings.yaml
profiler ratios --config config/my-settings.yaml
```

Check progress:

```bash
profiler status --config config/my-settings.yaml
```

Re-run a phase:

```bash
profiler reset --phase profile --config config/my-settings.yaml
profiler profile --config config/my-settings.yaml
```

## Project Structure

```
src/
  cli.py                  # Click CLI commands
  config.py               # YAML config loading and validation
  connections/
    mysql.py              # MySQL connection and query execution
    bigquery.py           # BigQuery connection with cost controls
    openmetadata.py       # Optional OpenMetadata SDK integration
  discovery/
    classifier.py         # Column classification (dimension/metric/identifier/temporal)
    schema.py             # INFORMATION_SCHEMA discovery
  profiling/
    profiler.py           # Per-column statistics computation
    sampler.py            # Adaptive sampling strategies by table size
  ratios/
    detector.py           # Plausible metric pair detection
    calculator.py         # Ratio computation, dimensional breakdowns, temporal trends
  storage/
    models.py             # SQLAlchemy ORM models (SQLite)
    store.py              # CRUD operations for profile database
```

## Testing

```bash
pip install -e ".[dev]"
pytest tests/
```

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
class BigQueryProjectConfig:
    project: str = ""
    datasets: list[str] = field(default_factory=list)


@dataclass
class BigQueryConfig:
    projects: list[BigQueryProjectConfig] = field(default_factory=list)
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


def _parse_bigquery_config(data: dict | None) -> BigQueryConfig:
    """Parse BigQuery config, supporting both single-project and multi-project formats."""
    if data is None:
        return BigQueryConfig()

    projects = []
    if "projects" in data:
        for p in data["projects"]:
            projects.append(_merge_dataclass(BigQueryProjectConfig, p))
    elif data.get("project"):
        # Backwards-compat: single project/datasets at top level
        projects.append(BigQueryProjectConfig(
            project=data["project"],
            datasets=data.get("datasets", []),
        ))

    return BigQueryConfig(
        projects=projects,
        max_bytes_per_query=data.get("max_bytes_per_query", 1_000_000_000),
        dry_run_threshold_bytes=data.get("dry_run_threshold_bytes", 500_000_000),
    )


def load_config(path: str) -> ProfilerConfig:
    """Load config from YAML file with environment variable overrides."""
    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    config = ProfilerConfig(
        mysql=_merge_dataclass(MySQLConfig, raw.get("mysql")),
        bigquery=_parse_bigquery_config(raw.get("bigquery")),
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

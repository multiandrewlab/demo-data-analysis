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
    def __init__(self, config: BigQueryConfig, project: str):
        self._config = config
        self._project = project
        self._client: bigquery.Client | None = None

    def _get_client(self) -> bigquery.Client:
        if self._client is None:
            self._client = bigquery.Client(project=self._project)
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
        project = self._project

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

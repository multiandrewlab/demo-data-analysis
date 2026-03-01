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
                # No PK available -- use LIMIT with deterministic ordering
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

"""Schema discovery from INFORMATION_SCHEMA and OpenMetadata."""

import logging
from typing import Any

from src.config import ProfilerConfig
from src.discovery.classifier import ColumnClassifier
from src.storage.store import ProfileStore

logger = logging.getLogger(__name__)


class SchemaDiscoverer:
    def __init__(self, config: ProfilerConfig, store: ProfileStore,
                 mysql_conn=None, bq_conns: dict | None = None):
        self._config = config
        self._store = store
        self._mysql = mysql_conn
        self._bq_conns = bq_conns or {}  # project -> BigQueryConnection
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

    def discover_bigquery(self, bq_conn, dataset: str):
        """Discover all tables and columns from a BigQuery dataset."""
        logger.info("Discovering BigQuery schema for dataset: %s", dataset)
        info = bq_conn.get_information_schema(dataset)

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
        """Run discovery for all configured MySQL databases and BQ projects/datasets."""
        if self._mysql:
            for db in self._config.mysql.databases:
                self.discover_mysql(db)
        for project_cfg in self._config.bigquery.projects:
            bq_conn = self._bq_conns.get(project_cfg.project)
            if bq_conn:
                for ds in project_cfg.datasets:
                    self.discover_bigquery(bq_conn, ds)

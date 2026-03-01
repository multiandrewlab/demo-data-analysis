"""MySQL connection with rate limiting and concurrency control."""

import logging
import threading
import time
from typing import Any
from urllib.parse import quote_plus

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
        user = quote_plus(config.user)
        password = quote_plus(config.password)
        return (f"mysql+pymysql://{user}:{password}"
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

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

        # Rule 1: Explicit PK/FK -> identifier
        if is_primary_key or is_foreign_key:
            return "identifier"

        # Rule 2: *_id naming convention -> identifier
        if name_lower.endswith("_id") or name_lower == "id":
            return "identifier"

        # Rule 3: Temporal types -> temporal
        if base in _TEMPORAL_TYPES:
            return "temporal"

        # Rule 4: Text/blob/JSON -> other
        if base in _TEXT_TYPES:
            return "other"

        # Rule 5: String types -> dimension (VARCHAR, CHAR, ENUM)
        if base in _STRING_TYPES:
            return "dimension"

        # Rule 6: Numeric columns -- check name patterns
        if base in _NUMERIC_TYPES:
            # Check metric name patterns first (more specific)
            for pattern in self._metric_patterns:
                if pattern in name_lower:
                    return "metric"
            # Check dimension name patterns
            for pattern in self._dim_patterns:
                if pattern in name_lower:
                    return "dimension"
            # Default numeric -> metric
            return "metric"

        # Rule 7: Boolean -> dimension
        if base in {"bool", "boolean", "bit"}:
            return "dimension"

        return "other"

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

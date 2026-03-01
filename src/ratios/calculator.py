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

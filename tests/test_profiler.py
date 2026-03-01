# tests/test_profiler.py
import json
import tempfile
import os
import pytest
import pandas as pd
import numpy as np
from src.profiling.profiler import ColumnProfiler


def test_profile_metric_column():
    """Profile a numeric/metric column and get correct stats."""
    np.random.seed(42)
    data = pd.DataFrame({"clicks": np.random.poisson(10, size=1000)})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["clicks"], classification="metric")

    assert result["null_rate"] == 0.0
    assert result["distinct_count"] > 5
    assert result["mean"] == pytest.approx(data["clicks"].mean(), rel=0.01)
    assert result["stddev"] == pytest.approx(data["clicks"].std(), rel=0.01)
    assert result["min_value"] == str(data["clicks"].min())
    assert result["max_value"] == str(data["clicks"].max())
    assert result["p5"] is not None
    assert result["p25"] is not None
    assert result["p75"] is not None
    assert result["p95"] is not None
    assert result["median"] is not None
    assert result["zero_rate"] is not None
    # Histogram should be a JSON string
    hist = json.loads(result["histogram_json"])
    assert len(hist) > 0
    assert all("bucket" in b and "count" in b for b in hist)


def test_profile_dimension_column():
    """Profile a categorical/dimension column and get value frequencies."""
    data = pd.DataFrame({"country": ["US"] * 500 + ["GB"] * 300 + ["DE"] * 200})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["country"], classification="dimension")

    assert result["distinct_count"] == 3
    assert result["null_rate"] == 0.0
    top = json.loads(result["top_values_json"])
    assert len(top) == 3
    assert top[0]["value"] == "US"
    assert top[0]["frequency"] == 500


def test_profile_temporal_column():
    """Profile a date/temporal column and get range info."""
    dates = pd.date_range("2024-01-01", periods=366, freq="D")
    data = pd.DataFrame({"event_date": dates})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["event_date"], classification="temporal")

    assert result["min_value"] == "2024-01-01"
    assert result["max_value"] == "2024-12-31"
    assert result["distinct_count"] == 366
    assert result["null_rate"] == 0.0


def test_profile_handles_nulls():
    """Profiler correctly reports null rates."""
    data = pd.DataFrame({"val": [1, 2, None, None, 5]})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["val"], classification="metric")
    assert result["null_rate"] == pytest.approx(0.4)


def test_profile_handles_all_zeros():
    """Profiler handles columns that are all zeros."""
    data = pd.DataFrame({"val": [0] * 100})
    profiler = ColumnProfiler()

    result = profiler.profile_column(data["val"], classification="metric")
    assert result["zero_rate"] == pytest.approx(1.0)
    assert result["mean"] == 0.0

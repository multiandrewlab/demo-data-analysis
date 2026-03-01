"""End-to-end integration test using mocked data sources."""

import json
import tempfile
import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock

from src.config import ProfilerConfig
from src.storage.store import ProfileStore
from src.discovery.schema import SchemaDiscoverer
from src.profiling.profiler import ColumnProfiler
from src.profiling.sampler import AdaptiveSampler
from src.ratios.detector import RatioDetector
from src.ratios.calculator import RatioCalculator


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    s = ProfileStore(db_path)
    yield s
    s.close()
    os.unlink(db_path)


@pytest.fixture
def sample_dataframe():
    """Realistic ad-tech sample data."""
    np.random.seed(42)
    n = 2000
    countries = np.random.choice(["US", "GB", "DE", "FR", "JP"], n,
                                  p=[0.35, 0.25, 0.15, 0.15, 0.10])
    publishers = np.random.choice(["pub_a", "pub_b", "pub_c"], n,
                                   p=[0.5, 0.3, 0.2])
    impressions = np.random.poisson(1000, n)

    ctr_map = {"US": 0.04, "GB": 0.03, "DE": 0.025, "FR": 0.02, "JP": 0.015}
    clicks = np.array([
        max(0, int(imp * ctr_map[c] * np.random.uniform(0.7, 1.3)))
        for imp, c in zip(impressions, countries)
    ])

    dates = pd.date_range("2024-01-01", periods=n, freq="h")

    return pd.DataFrame({
        "id": range(1, n + 1),
        "country": countries,
        "publisher": publishers,
        "impressions": impressions,
        "clicks": clicks,
        "event_date": dates,
    })


def test_full_pipeline(store, sample_dataframe):
    """End-to-end: discovery -> profiling -> ratio detection -> breakdowns."""
    config = ProfilerConfig()
    df = sample_dataframe

    # --- Phase 1: Discovery (manual, since we don't have real MySQL) ---
    table_id = store.upsert_table("mysql", "analytics", "ad_events", row_count=len(df))
    col_ids = {}
    col_classifications = {
        "id": ("bigint", "identifier", True, False),
        "country": ("varchar(2)", "dimension", False, False),
        "publisher": ("varchar(50)", "dimension", False, False),
        "impressions": ("int", "metric", False, False),
        "clicks": ("int", "metric", False, False),
        "event_date": ("date", "temporal", False, False),
    }
    for col_name, (dtype, cls, is_pk, is_fk) in col_classifications.items():
        col_ids[col_name] = store.add_column(
            table_id, col_name, dtype, classification=cls,
            is_primary_key=is_pk, is_foreign_key=is_fk,
        )

    # --- Phase 2: Profiling ---
    profiler = ColumnProfiler()
    for col_name, col_id in col_ids.items():
        if col_name == "id":
            continue
        cls = col_classifications[col_name][1]
        result = profiler.profile_column(df[col_name], classification=cls)
        store.save_column_profile(col_id, **result)

    store.update_table_status(table_id, "profile_status", "completed")

    # Verify profiling results
    imp_profile = store.get_column_profile(col_ids["impressions"])
    assert imp_profile is not None
    assert imp_profile.mean > 0
    assert imp_profile.stddev > 0

    country_profile = store.get_column_profile(col_ids["country"])
    assert country_profile is not None
    assert country_profile.distinct_count == 5
    top_values = json.loads(country_profile.top_values_json)
    assert top_values[0]["value"] == "US"  # Most frequent

    # --- Phase 3: Ratio Analysis ---
    detector = RatioDetector()
    calculator = RatioCalculator()

    metric_cols = ["impressions", "clicks"]
    pairs = detector.find_plausible_pairs(df, metric_cols)
    assert len(pairs) == 1
    assert pairs[0] == ("clicks", "impressions")

    num_col, den_col = pairs[0]

    # Global ratio
    ratio_result = calculator.compute_ratio(df, num_col, den_col)
    assert 0.01 < ratio_result["global_ratio"] < 0.1  # CTR should be ~3%

    ratio_id = store.save_ratio(
        table_id, col_ids[num_col], col_ids[den_col],
        global_ratio=ratio_result["global_ratio"],
        ratio_stddev=ratio_result["ratio_stddev"],
        ratio_histogram_json=ratio_result["ratio_histogram_json"],
    )

    # Dimensional breakdowns
    for dim_col_name in ["country", "publisher"]:
        breakdowns = calculator.compute_dimensional_breakdown(
            df, num_col, den_col, dim_col_name
        )
        for bd in breakdowns:
            store.save_dimensional_breakdown(
                ratio_id, col_ids[dim_col_name],
                bd["dimension_value"], bd["ratio_value"],
                sample_size=bd["sample_size"],
            )

    # Temporal trends
    trends = calculator.compute_temporal_trend(df, num_col, den_col, "event_date")
    for trend in trends:
        store.save_temporal_trend(ratio_id, trend["time_bucket"],
                                  trend["ratio_value"], sample_size=trend["sample_size"])

    # --- Verify final results ---
    ratios = store.get_ratios_for_table(table_id)
    assert len(ratios) == 1

    breakdowns = store.get_breakdowns_for_ratio(ratio_id)
    assert len(breakdowns) == 8  # 5 countries + 3 publishers

    country_breakdowns = [b for b in breakdowns
                          if b.dimension_column_id == col_ids["country"]]
    bd_map = {b.dimension_value: b.ratio_value for b in country_breakdowns}

    # US should have highest CTR, JP lowest
    assert bd_map["US"] > bd_map["JP"]
    assert bd_map["US"] == pytest.approx(0.04, abs=0.01)
    assert bd_map["JP"] == pytest.approx(0.015, abs=0.01)

    trends_stored = store.get_trends_for_ratio(ratio_id)
    assert len(trends_stored) > 0

    store.update_table_status(table_id, "ratio_status", "completed")

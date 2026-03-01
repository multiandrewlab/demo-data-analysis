# tests/test_ratio_calculator.py
import json
import pytest
import pandas as pd
import numpy as np
from src.ratios.calculator import RatioCalculator


@pytest.fixture
def sample_data():
    np.random.seed(42)
    n = 2000
    countries = np.random.choice(["US", "GB", "DE"], n, p=[0.5, 0.3, 0.2])
    impressions = np.random.poisson(1000, n)
    # CTR varies by country: US=4%, GB=3%, DE=2%
    ctr_by_country = {"US": 0.04, "GB": 0.03, "DE": 0.02}
    clicks = np.array([
        int(imp * ctr_by_country[c] * np.random.uniform(0.8, 1.2))
        for imp, c in zip(impressions, countries)
    ])
    dates = pd.date_range("2024-01-01", periods=n, freq="h")
    return pd.DataFrame({
        "impressions": impressions,
        "clicks": clicks,
        "country": countries,
        "event_date": dates,
    })


def test_compute_global_ratio(sample_data):
    """Computes correct global ratio (SUM(num)/SUM(den))."""
    calc = RatioCalculator()
    result = calc.compute_ratio(sample_data, "clicks", "impressions")

    expected = sample_data["clicks"].sum() / sample_data["impressions"].sum()
    assert result["global_ratio"] == pytest.approx(expected, rel=0.01)
    assert result["ratio_stddev"] is not None
    assert result["ratio_stddev"] > 0


def test_compute_dimensional_breakdown(sample_data):
    """Computes ratio broken down by a dimension column."""
    calc = RatioCalculator()
    breakdowns = calc.compute_dimensional_breakdown(
        sample_data, "clicks", "impressions", "country"
    )

    assert len(breakdowns) == 3
    bd_map = {b["dimension_value"]: b for b in breakdowns}
    assert "US" in bd_map
    assert "GB" in bd_map
    assert "DE" in bd_map

    # US should have highest CTR (~4%), DE lowest (~2%)
    assert bd_map["US"]["ratio_value"] > bd_map["DE"]["ratio_value"]
    assert bd_map["US"]["ratio_value"] == pytest.approx(0.04, abs=0.01)
    assert bd_map["DE"]["ratio_value"] == pytest.approx(0.02, abs=0.01)
    assert all(b["sample_size"] > 0 for b in breakdowns)


def test_compute_temporal_trend(sample_data):
    """Computes ratio trend over monthly time buckets."""
    calc = RatioCalculator()
    trends = calc.compute_temporal_trend(
        sample_data, "clicks", "impressions", "event_date", granularity="M"
    )

    assert len(trends) > 0
    assert all("time_bucket" in t for t in trends)
    assert all("ratio_value" in t for t in trends)
    assert all("sample_size" in t for t in trends)


def test_compute_ratio_histogram(sample_data):
    """Computes a histogram of the row-level ratio distribution."""
    calc = RatioCalculator()
    result = calc.compute_ratio(sample_data, "clicks", "impressions")

    hist = json.loads(result["ratio_histogram_json"])
    assert len(hist) > 0
    total_count = sum(b["count"] for b in hist)
    assert total_count > 0

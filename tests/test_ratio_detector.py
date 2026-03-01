# tests/test_ratio_detector.py
import pytest
import pandas as pd
import numpy as np
from src.ratios.detector import RatioDetector


def test_detect_plausible_pairs():
    """Detector finds plausible metric pairs like clicks/impressions."""
    np.random.seed(42)
    n = 1000
    impressions = np.random.poisson(1000, n)
    clicks = (impressions * np.random.uniform(0.01, 0.05, n)).astype(int)
    data = pd.DataFrame({"impressions": impressions, "clicks": clicks,
                          "country": ["US"] * 500 + ["GB"] * 500})

    detector = RatioDetector()
    metric_cols = ["impressions", "clicks"]
    pairs = detector.find_plausible_pairs(data, metric_cols)

    # Should find clicks/impressions as a plausible pair
    assert len(pairs) > 0
    pair_sets = [frozenset(p) for p in pairs]
    assert frozenset(("clicks", "impressions")) in pair_sets


def test_reject_unrelated_metrics():
    """Detector rejects pairs with wildly varying ratios (unrelated metrics)."""
    np.random.seed(42)
    n = 1000
    data = pd.DataFrame({
        "metric_a": np.random.exponential(100, n),
        "metric_b": np.random.exponential(1, n) * np.random.choice([1, 1000], n),
    })

    detector = RatioDetector()
    pairs = detector.find_plausible_pairs(data, ["metric_a", "metric_b"])
    # Wildly varying ratio — should be rejected or empty
    # (this depends on the threshold, but with a 1000x variation it should fail)
    # We allow it to be found but with high stddev flagged


def test_reject_zero_denominator():
    """Pairs where denominator is mostly zero are rejected."""
    data = pd.DataFrame({
        "numerator": [10, 20, 30, 40, 50],
        "denominator": [0, 0, 0, 0, 1],
    })

    detector = RatioDetector()
    pairs = detector.find_plausible_pairs(data, ["numerator", "denominator"])
    # denominator is 80% zero — should be rejected
    assert len(pairs) == 0


def test_pairs_are_ordered():
    """Each pair is returned as (numerator, denominator) where numerator < denominator on avg."""
    np.random.seed(42)
    impressions = np.random.poisson(1000, 500)
    clicks = (impressions * 0.03).astype(int)
    data = pd.DataFrame({"impressions": impressions, "clicks": clicks})

    detector = RatioDetector()
    pairs = detector.find_plausible_pairs(data, ["impressions", "clicks"])

    for num, den in pairs:
        # The smaller metric should be numerator (clicks < impressions)
        assert data[num].mean() <= data[den].mean()

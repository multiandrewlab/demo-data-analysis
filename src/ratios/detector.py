"""Detect plausible metric pairs for ratio analysis."""

import logging
from itertools import combinations

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Minimum fraction of non-zero values in denominator
MIN_NONZERO_RATE = 0.5
# Maximum coefficient of variation for the ratio to be considered "bounded"
MAX_RATIO_CV = 10.0


class RatioDetector:
    def find_plausible_pairs(self, data: pd.DataFrame,
                             metric_columns: list[str],
                             min_nonzero_rate: float = MIN_NONZERO_RATE,
                             max_ratio_cv: float = MAX_RATIO_CV,
                             ) -> list[tuple[str, str]]:
        """Find pairs of metric columns that form plausible ratios.

        Returns list of (numerator, denominator) tuples where the numerator
        is the column with the smaller mean (e.g., clicks/impressions not
        impressions/clicks).
        """
        plausible = []

        for col_a, col_b in combinations(metric_columns, 2):
            a = pd.to_numeric(data[col_a], errors="coerce").dropna()
            b = pd.to_numeric(data[col_b], errors="coerce").dropna()

            if len(a) == 0 or len(b) == 0:
                continue

            # Try both orderings — the smaller mean is the numerator
            if a.mean() <= b.mean():
                num_col, den_col = col_a, col_b
                num, den = a, b
            else:
                num_col, den_col = col_b, col_a
                num, den = b, a

            # Check: denominator should be mostly non-zero
            nonzero_rate = (den != 0).sum() / len(den)
            if nonzero_rate < min_nonzero_rate:
                logger.debug("Rejecting %s/%s: denominator %.0f%% zero",
                            num_col, den_col, (1 - nonzero_rate) * 100)
                continue

            # Check: both should be non-negative
            if (num < 0).any() or (den < 0).any():
                logger.debug("Rejecting %s/%s: contains negative values",
                            num_col, den_col)
                continue

            # Align indices first (dropna may have removed different rows)
            common_idx = num.index.intersection(den.index)
            num_aligned = num.loc[common_idx]
            den_aligned = den.loc[common_idx]

            # Compute ratio where denominator is non-zero
            mask = den_aligned != 0
            num_aligned = num_aligned.loc[mask]
            den_aligned = den_aligned.loc[mask]

            if len(den_aligned) < 10:
                continue

            ratio = num_aligned / den_aligned
            ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()

            if len(ratio) < 10:
                continue

            # Check: ratio should be reasonably bounded (low CV)
            cv = ratio.std() / ratio.mean() if ratio.mean() != 0 else float("inf")
            if cv > max_ratio_cv:
                logger.debug("Rejecting %s/%s: ratio CV=%.2f too high",
                            num_col, den_col, cv)
                continue

            plausible.append((num_col, den_col))
            logger.info("Found plausible ratio: %s / %s (mean=%.4f, cv=%.2f)",
                       num_col, den_col, ratio.mean(), cv)

        return plausible

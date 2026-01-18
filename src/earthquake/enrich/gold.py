# src/earthquake/enrich/gold.py
from __future__ import annotations

import pandas as pd


def silver_to_gold_df(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Simple daily rollup. This is your "gold" dataset."""
    if silver_df.empty:
        return pd.DataFrame(
            columns=["event_date", "quake_count", "max_magnitude", "avg_magnitude", "min_magnitude"]
        )

    gold = (
        silver_df.groupby("event_date", dropna=False)
        .agg(
            quake_count=("event_id", "count"),
            max_magnitude=("magnitude", "max"),
            avg_magnitude=("magnitude", "mean"),
            min_magnitude=("magnitude", "min"),
        )
        .reset_index()
        .sort_values("event_date", na_position="last")
    )
    return gold


def add_sig_class(df: pd.DataFrame, *, low: float = 100.0, high: float = 500.0) -> pd.DataFrame:
    """
    Adds a 'sig_class' column based on 'sig'.
    Boundaries match the test:
      sig <= low      -> Low
      low < sig <= high -> Moderate
      sig > high      -> High
    """
    out = df.copy()
    sig = pd.to_numeric(out["sig"], errors="coerce")

    out["sig_class"] = pd.NA
    out.loc[sig <= low, "sig_class"] = "Low"
    out.loc[(sig > low) & (sig <= high), "sig_class"] = "Moderate"
    out.loc[sig > high, "sig_class"] = "High"
    return out

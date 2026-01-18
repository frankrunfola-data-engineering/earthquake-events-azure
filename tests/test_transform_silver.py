from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from earthquake.transform.silver import raw_JSON_to_silver_df


def test_raw_JSON_to_silver_df_basic_columns():
    features = [
        {
            "id": "abc123",
            "geometry": {"coordinates": [-118.5, 34.2, 10.0]},
            "properties": {
                "time": 1730000000000,
                "mag": 4.2,
                "place": "Somewhere",
                "title": "M 4.2 - Somewhere",
                "url": "https://example.com/event/abc123",
                "sig": 250,  # ok to include; function just ignores it
            },
        }
    ]

    run_date = date.today()
    bronze_dict = {"features": features}
    df = raw_JSON_to_silver_df(bronze_dict, run_date=run_date)

    expected_cols = [
        "event_id",
        "event_time_utc",
        "event_date",
        "magnitude",
        "place",
        "title",
        "url",
        "longitude",
        "latitude",
        "depth_km",
        "run_date",
    ]
    assert list(df.columns) == expected_cols
    assert len(df) == 1

    assert df.loc[0, "event_id"] == "abc123"
    assert df.loc[0, "longitude"] == -118.5
    assert df.loc[0, "latitude"] == 34.2
    assert df.loc[0, "depth_km"] == 10.0
    assert df.loc[0, "magnitude"] == 4.2

    assert pd.notna(df.loc[0, "event_time_utc"])
    assert pd.notna(df.loc[0, "event_date"])
    assert df.loc[0, "run_date"] == run_date.isoformat()


def test_raw_JSON_to_silver_df_empty_returns_empty_df():
    bronze: dict[str, Any] = {"features": []}
    df = raw_JSON_to_silver_df(bronze, run_date=date.today())
    assert df.empty

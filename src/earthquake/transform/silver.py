# src/earthquake/transform/silver.py
from __future__ import annotations

from datetime import UTC, datetime
from datetime import date as date_type
from typing import Any

import pandas as pd


def _ms_to_utc_iso(ms: Any) -> str | None:
    if ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000.0, tz=UTC)
        return dt.isoformat()
    except Exception:
        return None


def _utc_date(iso_ts: str | None) -> str | None:
    if not iso_ts:
        return None
    try:
        return iso_ts.split("T", 1)[0]
    except Exception:
        return None


def raw_JSON_to_silver_df(bronze: dict, *, run_date: date_type) -> pd.DataFrame:
    """
    Flatten USGS GeoJSON -> tabular "silver" dataframe.
    """
    features = bronze.get("features") or []
    rows: list[dict] = []

    for f in features:
        props = f.get("properties") or {}
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None, None]  # [lon, lat, depth_km]

        event_time_utc = _ms_to_utc_iso(props.get("time"))
        rows.append(
            {
                "event_id": f.get("id"),
                "event_time_utc": event_time_utc,
                "event_date": _utc_date(event_time_utc),
                "magnitude": props.get("mag"),
                "place": props.get("place"),
                "title": props.get("title"),
                "url": props.get("url"),
                "longitude": coords[0] if len(coords) > 0 else None,
                "latitude": coords[1] if len(coords) > 1 else None,
                "depth_km": coords[2] if len(coords) > 2 else None,
                "run_date": run_date.isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Optional light cleanup (safe)
    if not df.empty:
        df["magnitude"] = pd.to_numeric(df["magnitude"], errors="coerce")
        df["depth_km"] = pd.to_numeric(df["depth_km"], errors="coerce")

    return df

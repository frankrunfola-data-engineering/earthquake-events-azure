# src/earthquake/extract/usgs.py
from __future__ import annotations

import json
from datetime import date
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def get_API_data(*, base_url: str, start_date: date, end_date: date) -> dict:
    """
    Fetch USGS earthquake GeoJSON for a date window.
    """
    params = {
        "format": "geojson",
        "starttime": start_date.isoformat(),
        "endtime": end_date.isoformat(),
    }
    url = f"{base_url}?{urlencode(params)}"

    req = Request(url, headers={"User-Agent": "earthquake-pipeline/1.0"})
    with urlopen(req, timeout=30) as resp:
        payload = resp.read().decode("utf-8")

    return json.loads(payload)

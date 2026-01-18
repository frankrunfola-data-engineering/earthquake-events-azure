from __future__ import annotations

import json
from datetime import date
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def get_API_data(*, base_url: str, start_date: date, end_date: date) -> dict[str, Any]:
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

    data = json.loads(payload)
    if not isinstance(data, dict):
        raise TypeError(f"Expected USGS response to be a JSON object, got {type(data).__name__}")

    return data

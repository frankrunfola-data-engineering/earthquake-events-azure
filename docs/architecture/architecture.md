# Architecture

This repo demonstrates a simple **medallion pipeline** using public **USGS** earthquake data.

```mermaid
graph LR
  A[USGS API] -->|GeoJSON features| B[Bronze: JSON]
  B --> C[Silver: normalized CSV]
  C --> D[Gold: enriched CSV\n(country_code, sig_class)]
```

## Key design choices
- **Low coupling**: extraction, transforms, enrichment, and I/O are separate modules.
- **Pure transforms**: `raw_JSON_to_silver_df` is I/O-free for easy testing.
- **Offline enrichment**: country code enrichment uses `reverse_geocoder` (no external calls).

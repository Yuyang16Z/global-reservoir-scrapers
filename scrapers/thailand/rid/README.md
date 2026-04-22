# Thailand RID (Royal Irrigation Department) scraper

Scrapes <https://app.rid.go.th/reservoir/> — 35 large dams + 448 medium reservoirs.

## Run locally

```bash
# Default: yesterday + today (Bangkok time)
python scrapers/thailand/rid/thailand_rid_scraper.py

# Custom output dir
OUTPUT_DIR=/tmp/thailand python scrapers/thailand/rid/thailand_rid_scraper.py

# Backfill a range
THAILAND_START_DATE=2024-01-01 THAILAND_END_DATE=2024-12-31 \
  python scrapers/thailand/rid/thailand_rid_scraper.py
```

## Output layout

```
<OUTPUT_DIR>/
├── metadata/
│   └── thailand_reservoirs.csv                           # 1 row / reservoir
├── timeseries/daily/
│   └── thailand_timeseries_YYYY-MM-DD.csv                # slim per-day snapshot
└── raw/
    ├── dams/YYYY-MM-DD.json                              # raw API payload (audit)
    └── rsvmiddles/YYYY-MM-DD.json
```

## Column conventions

See top-level [`schema.md`](../../../schema.md). All storage / inflow / outflow volumes
are in **million m³** (ล้าน ลบ.ม.) per RID's convention — no unit conversion applied.

## Env vars

| Var | Default | Meaning |
|---|---|---|
| `OUTPUT_DIR` | `./thailand_rid_outputs` next to this script | Where to write outputs |
| `THAILAND_START_DATE` / `THAILAND_END_DATE` | unset | Inclusive backfill range (`YYYY-MM-DD`). If unset, scraper fetches yesterday+today (Bangkok) |
| `SKIP_EXISTING_DAILY` | `1` | Skip dates whose CSV already exists. Set `0` to overwrite |
| `THAILAND_SLEEP` | `1.2` | Seconds between date requests (only matters for backfill) |

## Known quirks

- Some rows come back with `" - "` for missing values — cleaned to empty.
- `inflow_daily` is the daily net inflow in million m³/day (NOT m³/s).
- Large-dam `DAM_QMax` (total capacity incl. freeboard) is often larger than `DAM_QStore`
  (capacity at FRL); both are exposed as `capacity_total` and `storage_capacity`.
- Medium reservoirs use `cresv` as ID (like `rsv01`, `rsv02`, …) — NOT globally unique
  across countries, prefix with country in downstream normalize step if needed.

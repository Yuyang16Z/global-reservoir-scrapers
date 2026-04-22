# Taiwan WRA scraper

API-first scraper for Taiwan reservoir data from the Water Resources Agency (WRA).

## Sources

- Current daily operation status:
  `https://opendata.wra.gov.tw/api/v2/51023e88-4c76-4dbc-bbb9-470da690d539?format=JSON&sort=_importdate+asc`
- Current water level dataset:
  `https://opendata.wra.gov.tw/api/v2/2be9044c-6e44-4856-aad5-dd108c2e6679?format=JSON&sort=_importdate+asc`
- Annual reservoir basic information:
  `https://opendata.wra.gov.tw/api/v2/708a43b0-24dc-40b7-9ed2-fca6a291e7ae?format=JSON&sort=_importdate+asc`
- Historical daily endpoint:
  `https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date=YYYY-MM-DD`

## What it writes

- `metadata/taiwan_wra_reservoirs.csv`
- `timeseries/daily/taiwan_timeseries_YYYY-MM-DD.csv`
- `raw/static_reservoirs.json`
- `raw/daily/YYYY-MM-DD.json`
- `raw/realtime_YYYY-MM-DD.json`
- `run_logs/<timestamp>_summary.json`

## Notes

- Default run fetches **yesterday + today** in Taiwan time.
- Manual backfill is supported via `TAIWAN_START_DATE` and `TAIWAN_END_DATE`.
- The historical `fhy.wra.gov.tw` daily endpoint supplies the per-date values.
- The `opendata.wra.gov.tw` current datasets supply names, current water level,
  storage percentage, and metadata enrichment.
- Current version keeps `lat` / `lon` blank in metadata because the API set used here
  does not expose coordinates directly. Taiwan has separate location datasets that can
  be joined later.

## Run locally

```bash
python scrapers/taiwan/wra/taiwan_wra_scraper.py

# Backfill a range
TAIWAN_START_DATE=2026-04-01 TAIWAN_END_DATE=2026-04-22 \
python scrapers/taiwan/wra/taiwan_wra_scraper.py
```

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `OUTPUT_DIR` | `./taiwan_wra_outputs` next to the script | Where outputs are written |
| `TAIWAN_START_DATE` / `TAIWAN_END_DATE` | unset | Inclusive date range (`YYYY-MM-DD`) |
| `SKIP_EXISTING_DAILY` | `1` | Skip existing daily CSVs. Set `0` to overwrite |
| `SAVE_RAW_JSON` | `1` | Save raw JSON payloads for audit/debug |

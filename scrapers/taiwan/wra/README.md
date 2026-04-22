# Taiwan WRA scraper

API-first scraper for Taiwan reservoir data from the Water Resources Agency (WRA).

## Sources

- Static reservoir name mapping:
  `https://data.wra.gov.tw/Service/OpenData.aspx?format=json&id=E2_7_00001`
- Daily reservoir operations:
  `https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date=YYYY-MM-DD`
- Current reservoir realtime snapshot:
  `https://fhy.wra.gov.tw/WraApi/v1/Reservoir/RealTime`

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
- The daily endpoint supplies the core daily values; the realtime endpoint augments
  the current date with water level / storage percentage / detailed outflow fields.
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

# China MWR API scraper

Scrapes the Ministry of Water Resources large-reservoir realtime page:
<http://xxfb.mwr.cn/sq_dxsk.html?v=1.0>.

This replaces the heavy Selenium screenshot/OCR path for steady-state runs. The
site's table data is available from the public JSON endpoint used by the page,
but Chinese and numeric values are wrapped in a custom-font obfuscation tag. The
scraper decodes that payload by:

1. inferring digit glyphs from encoded numeric fields;
2. training Chinese text glyphs from existing OCR CSVs in `data/china/mwr`;
3. applying a small `idNo` correction table for rare station-name characters.

No login, captcha, or private endpoint is used.

## Output

```
data/china/mwr_api/
├── metadata/china_mwr_api_reservoirs.csv
├── timeseries/daily/china_mwr_api_timeseries_YYYY-MM-DD.csv
├── raw/china_mwr_api_raw_YYYY-MM-DD_<timestamp>.json
└── run_logs/<timestamp>_summary.json
```

The source exposes water level and daily water-level change only. Capacity,
inflow, and outflow are not published on this page.

## Run locally

```bash
python scrapers/china/mwr_api/china_mwr_api_scraper.py
```

To test without writing into the repo data tree:

```bash
python scrapers/china/mwr_api/china_mwr_api_scraper.py --output-dir /tmp/china_mwr_api
```

## Cadence

The GitHub Actions workflow runs once daily at 12:30 UTC, which is 20:30
Beijing time. The source is a current snapshot rather than a historical API, so
daily polling is still needed to build a durable archive.

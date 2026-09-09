# China MWR API scraper

Scrapes the Ministry of Water Resources large-reservoir realtime page:
<http://xxfb.mwr.cn/sq_dxsk.html?v=1.0>.

This replaces the heavy Selenium screenshot/OCR path for steady-state runs. The
site's table data is available from the public JSON endpoint used by the page,
but Chinese and numeric values are wrapped in a custom-font obfuscation tag. The
scraper decodes that payload by:

1. inferring digit glyphs from encoded numeric fields;
2. training Chinese text glyphs from the read-only OCR CSV archive in
   `data/china/mwr_ocr_archive`;
3. applying a small `idNo` correction table for rare station-name characters.

No login, captcha, or private endpoint is used.

The scraper first calls the official endpoint directly. If the GitHub-hosted
runner has no network route to the MWR host, it retries the same public URL via
Jina Reader as a transport relay. The saved raw object is still the official
MWR JSON payload, and each run summary records the actual transport and fetch
URL. This fallback was added after seven consecutive GitHub runner routing
failures on 2026-09-02 through 2026-09-08 while direct access from China-facing
networks remained healthy.

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

The GitHub Actions workflow runs at 04:30 and 12:30 UTC, which is 12:30 and
20:30 Beijing time. The source is a current snapshot rather than a historical
API, so both runs read the same daily table and provide two chances to archive
it before replacement.

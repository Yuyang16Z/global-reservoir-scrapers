# Japan OpenGov Dam Scraper

This scraper collects Japan dam reservoir data from:

- Listing page: `https://opengov.jp/en/geo/dam-reservoir/`
- Per-dam detail page: `https://opengov.jp/en/geo/dam-reservoir/<slug>/`
- Upstream source noted on the pages: `国土交通省 水文水質データベース`

Outputs:

- `data/japan/opengov/metadata/japan_opengov_reservoirs.csv`
- `data/japan/opengov/timeseries/daily/japan_opengov_timeseries_YYYY-MM-DD.csv`
- `data/japan/opengov/run_logs/*_summary.json`

Notes:

- `Storage Rate (%)` and `Storage Volume (千m^3)` come from embedded chart JSON and support multi-year backfill.
- `Inflow (m^3/s)` and `Outflow (m^3/s)` are only available for the recent visible daily table window, so older rows are usually blank for those fields.
- `lat` / `lon` are currently blank because the page does not expose coordinates directly.

Examples:

```bash
OUTPUT_DIR="$(pwd)/data/japan/opengov" python scrapers/japan/opengov/japan_opengov_scraper.py
```

```bash
OUTPUT_DIR="$(pwd)/data/japan/opengov" JAPAN_START_DATE=2021-04-22 JAPAN_END_DATE=2026-04-22 python scrapers/japan/opengov/japan_opengov_scraper.py
```

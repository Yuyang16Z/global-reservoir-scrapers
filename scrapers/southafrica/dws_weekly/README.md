# South Africa DWS Weekly Reservoir Scraper

Scrapes the **South African Department of Water and Sanitation (DWS) Weekly
State of the Reservoirs** bulletin and parses the per-reservoir state table.

- Bulletin landing page: <https://www.dws.gov.za/Hydrology/Weekly/Province.aspx>
- Dated archive pattern: `https://www.dws.gov.za/drought/docs/Weekly{YYYYMMDD}.pdf`
  (filename date = report Monday)

## Cadence

DWS publishes one bulletin per Monday (sometimes shifted by holidays). The
GitHub Actions workflow runs **weekly on Tuesday 06:00 UTC** — comfortably
after DWS's Monday publish window. Polling more often would not yield new
data; weekly is the source's natural cadence.

Some Mondays have no bulletin (DWS skips holidays / occasionally goes
quiet); the scraper detects placeholder PDFs (~1.4 KB stub vs ~800 KB real
bulletin) and continues without committing those weeks.

## Outputs

```
data/southafrica/dws_weekly/
  pdfs/Weekly{YYYYMMDD}.pdf                       # raw PDFs (audit trail)
  timeseries/timeseries_long.csv                  # cumulative long-format
  metadata/southafrica_dws_reservoirs.csv         # rebuilt every run
  run_logs/{date}_summary.json
```

`timeseries/timeseries_long.csv` columns:

| Column | Meaning |
|---|---|
| `date` | YYYYMMDD report-date (Monday-of-week) |
| `station_id` | DWS station code, e.g. `A2R001` |
| `reservoir` | Reservoir name |
| `river`, `wma`, `prov`, `wss`, `district_mun` | Geographic / admin codes |
| `fsc_mcm` | Full Supply Capacity (10⁶ m³) |
| `water_mcm` | Water in Dam (10⁶ m³) |
| `pct_last_year`, `pct_last_week`, `pct_full` | Percent-full at three time points |

`metadata/southafrica_dws_reservoirs.csv` aggregates per-station info
from the cumulative timeseries (most-recent non-empty values for
geo/admin columns; **median** of `fsc_mcm` to absorb rounding drift
between weekly bulletins).

## Idempotency / safety

- Already-downloaded PDFs are reused from `pdfs/` (no re-fetch).
- Already-parsed dates are skipped (no duplicate rows in timeseries).
- Placeholder PDFs (DWS's ~1.4 KB stub for unpublished dates) are
  ignored — neither saved nor parsed.

## Manual / backfill

Default window scans the last 24 months of Mondays. Override via env vars:

```bash
DWS_START_DATE=2024-01-01 DWS_END_DATE=2026-04-27 python3 \
  scrapers/southafrica/dws_weekly/dws_weekly_scraper.py
```

The first run on a fresh checkout does the full backfill; subsequent
runs only fetch new weeks.

## Out of scope

- **Daily resolution**: DWS's "Daily State of Dams" is a regional MAP
  IMAGE per WMA (not tabular data); fine for visual reference but
  impractical to OCR for 229 stations × 4664 dates.
- **Pre-2021 weekly data**: this scraper checks the
  `/drought/docs/Weekly{YYYYMMDD}.pdf` archive — DWS keeps PDFs back
  to roughly 2014 in that path. The default 24-month window can be
  widened via `DWS_START_DATE`.
- **Daily 1922-onwards historical archive**: lives in DWS's Verified
  data portal (`HyData.aspx`) — a separate one-shot scraper exists in
  the local research workspace at
  `~/Desktop/work/resovoir data/SouthAfrica/dws_verified_scraper.py`,
  kept LOCAL only per the dataset's data-routing rule (historical
  archives → Desktop; ephemeral weekly snapshots → git repo).

## License

DWS publishes these bulletins as open public information without an
explicit license; downstream users should attribute the **South
African Department of Water and Sanitation (DWS)**.

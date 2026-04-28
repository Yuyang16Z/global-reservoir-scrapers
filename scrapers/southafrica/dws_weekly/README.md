# South Africa DWS Weekly Reservoir Scraper

Scrapes the **South African Department of Water and Sanitation (DWS) Weekly
State of the Reservoirs** bulletin for the most recent published week and
parses the per-reservoir state table.

- Bulletin landing page: <https://www.dws.gov.za/Hydrology/Weekly/Province.aspx>
- Dated archive pattern: `https://www.dws.gov.za/drought/docs/Weekly{YYYYMMDD}.pdf`
  (filename date = report Monday)

## Cadence

DWS publishes one bulletin per Monday (sometimes shifted by holidays).
The GitHub Actions workflow runs **weekly on Tuesday 06:00 UTC** —
comfortably after DWS's Monday publish window. Polling more often
would not yield new data; weekly is the source's natural cadence.

If the most recent Monday has no bulletin yet (~1.4 KB placeholder PDF
returned), the scraper walks back up to 8 Mondays to find the latest
real bulletin and uses that.

## Output (each run replaces previous)

```
data/southafrica/dws_weekly/
  timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv   # the latest snapshot (one file)
  metadata/southafrica_dws_reservoirs.csv            # one row per reservoir
  run_logs/{run_date}_summary.json                   # short JSON log
```

The repo carries **only the most recent scrape's output**. Older
snapshot files in `timeseries/` are deleted on each run (cumulative
history is built downstream by the user's own pipeline, not in this
repo). The PDF cache lives in `pdfs/` but is gitignored.

`timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv` columns:

| Column | Meaning |
|---|---|
| `date` | YYYYMMDD report-date (Monday-of-week) |
| `station_id` | DWS station code, e.g. `A2R001` |
| `reservoir` | Reservoir name |
| `river`, `wma`, `prov`, `wss`, `district_mun` | Geographic / admin codes |
| `fsc_mcm` | Full Supply Capacity (10⁶ m³) |
| `water_mcm` | Water in Dam (10⁶ m³) |
| `pct_last_year`, `pct_last_week`, `pct_full` | Percent-full at three time points |

## Manual override

Scrape a specific Monday (must be a Monday with a real bulletin):

```bash
DWS_TARGET_DATE=2026-04-13 python3 \
  scrapers/southafrica/dws_weekly/dws_weekly_scraper.py
```

Keep prior snapshot files instead of replacing them (one-off audit run):

```bash
DWS_KEEP_OLD=1 python3 scrapers/southafrica/dws_weekly/dws_weekly_scraper.py
```

## Out of scope

- **Cumulative timeseries history**: handled by the user's downstream
  pipeline, not by this repo.
- **Daily resolution**: DWS's "Daily State of Dams" is a regional MAP
  IMAGE per WMA (not tabular data); fine for visual reference but
  impractical to OCR for 229 stations × thousands of dates.
- **Daily 1922-onwards historical archive**: lives in DWS's Verified
  data portal (`HyData.aspx`) — a separate one-shot scraper exists in
  the local research workspace at
  `~/Desktop/work/resovoir data/SouthAfrica/dws_verified_scraper.py`,
  kept LOCAL only per the dataset's data-routing rule.

## License

DWS publishes these bulletins as open public information without an
explicit license; downstream users should attribute the **South
African Department of Water and Sanitation (DWS)**.

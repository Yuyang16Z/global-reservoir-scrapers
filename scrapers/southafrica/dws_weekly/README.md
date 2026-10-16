# South Africa DWS Weekly Reservoir Scraper

Scrapes the **South African Department of Water and Sanitation (DWS) Weekly
State of the Reservoirs** bulletin for the most recent published week and
parses the per-reservoir state table.

- Bulletin landing page: <https://www.dws.gov.za/Hydrology/Weekly/Province.aspx>
- Dated archive pattern: `https://www.dws.gov.za/drought/docs/Weekly{YYYYMMDD}.pdf`
  (filename date = report Monday)
- DWS-derived fallback: <https://reservoirs.earth/data/south-africa.json>
  (used only when the official host rejects or fails direct downloads)

The official PDF remains the preferred source. Since June 2026 the DWS host
has intermittently returned HTTP 403 to automated clients. When that happens,
the scraper falls back to the public `reservoirs.earth` JSON mirror, which
attributes its measurements to DWS and publishes them under CC BY 4.0. The
fallback is matched to the existing DWS station metadata by normalized
reservoir name; unmatched and missing records are listed in the run log.

## Cadence

DWS publishes one bulletin per Monday (sometimes shifted by holidays).
The GitHub Actions workflow runs on **Tuesday 06:00 UTC** — comfortably
after DWS's Monday publish window — and again on Thursday as the recovery
run. Polling more often would not yield new data; weekly is the source's
natural cadence.

If the most recent Monday has no bulletin yet (~1.4 KB placeholder PDF
returned), the scraper walks back up to 8 Mondays to find the latest
real bulletin and uses that. HTTP status, content type and response size for
all official URL probes are retained in the run diagnostics.

## Output (snapshots accumulate over time)

```
data/southafrica/dws_weekly/
  raw/reservoirs_earth_south_africa_{YYYYMMDD}.json  # mirror country JSON (newest-reading runs)
  raw/reservoirs_earth_history_{YYYYMMDD}.csv        # mirror history readings read for that date
  timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv   # one file per bulletin
  metadata/southafrica_dws_reservoirs.csv            # latest official snapshot's reservoirs
  metadata/mirror_history_dates.csv                  # every mirror history date decided, and why
  run_logs/{run_date}[_target_date]_summary.json     # short JSON log
```

Each run adds a new dated snapshot CSV alongside any prior weeks —
the repo is the durable archive going forward. Re-running on a date
already present is a no-op (the snapshot file's existence is the
idempotency check). The PDF cache lives in `pdfs/` but is gitignored.

## Mirror catch-up and guards

The mirror's country JSON carries only each dam's newest reading, so one run
takes one bulletin. When the mirror catches up several weeks at once, the
weeks in between exist only on its per-dam history pages: the 2026-07-27 and
2026-09-21 bulletins were lost that way until dated re-runs recovered them on
2026-10-15. Every mirror run therefore also:

1. reads the history page of the largest dam in each of the six regions with
   most dams and lists the dates on them;
2. for every date that is neither in `timeseries/` nor in
   `metadata/mirror_history_dates.csv`, reads every dam's history page once
   and builds that bulletin;
3. writes the bulletin only when it has at least 150 matched dams and does not
   repeat an archived bulletin (90% or more of the stations both carry with the
   same percentage, within 21 days). In June 2026 the mirror dated readings by
   its own capture day, so the official 2026-06-01 bulletin reappears as 06-03,
   06-07 and 06-08, and 2026-06-08 as 06-09 and 06-16. Genuine consecutive DWS
   bulletins share at most 61% (2021-2026);
4. records every decision (`backfilled`, `rejected_repeats_bulletin`,
   `rejected_too_few_dams`) in `metadata/mirror_history_dates.csv`, so a date is
   evaluated once, and keeps the readings it read, with each page's SHA-256, in
   `raw/reservoirs_earth_history_{YYYYMMDD}.csv`, rejected dates included.

If any history page fails during that pass, nothing is decided and the next run
tries again. The repeat guard also applies to the newest-reading bulletin and to
`DWS_TARGET_DATE` runs; a rejected bulletin keeps its raw evidence, but no
snapshot is written.

**A dam listed twice by the mirror.** The mirror lists Elandsdrift (`Q5R001`) as
"Elands Drift Dam" and "Elandsdrift Dam", at the same coordinates with different
readings, and the first entry with a reading used to win. When exactly one entry
reproduces at least two official bulletins (a reading within 10 days carrying the
bulletin's percentage, taken from snapshots parsed from the official PDFs), only
that entry is read; "Elandsdrift Dam" reproduces three, the other none. Without
that evidence the old rule stands. Each run logs the entries and their scores
under `mirror_diagnostics.duplicate_entries`. Snapshots archived before this rule
keep the values they were written with.

Fallback snapshots preserve the existing DWS station IDs and metadata.
`pct_last_year` and `pct_last_week` are left blank because the fallback JSON
does not expose those fields; `pct_full` and `water_mcm` remain populated.

`timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv` columns:

| Column | Meaning |
|---|---|
| `date` | YYYYMMDD source report date |
| `station_id` | DWS station code, e.g. `A2R001` |
| `reservoir` | Reservoir name |
| `river`, `wma`, `prov`, `wss`, `district_mun` | Geographic / admin codes |
| `fsc_mcm` | Full Supply Capacity (10⁶ m³) |
| `water_mcm` | Water in Dam (10⁶ m³) |
| `pct_last_year`, `pct_last_week`, `pct_full` | Percent-full at three time points |

## Manual override

Scrape or backfill a specific source report date. If the official PDF is
blocked, the fallback reads the per-reservoir historical pages and requires
at least 150 matched reservoirs before writing a snapshot; the repeat guard
above applies too, and the readings used are kept in
`raw/reservoirs_earth_history_{YYYYMMDD}.csv`:

```bash
DWS_TARGET_DATE=2026-09-21 python3 \
  scrapers/southafrica/dws_weekly/dws_weekly_scraper.py
```

`DWS_DRY_RUN=1` runs the same fetches and decisions and prints the run summary
without writing anything.

## Out of scope
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
African Department of Water and Sanitation (DWS)**. Fallback JSON is provided
by `reservoirs.earth` under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

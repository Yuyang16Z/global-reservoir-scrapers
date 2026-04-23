# Philippines PAGASA dam scraper

Source: [https://www.pagasa.dost.gov.ph/flood](https://www.pagasa.dost.gov.ph/flood)

Covers 9 major Luzon dams: Angat, Ipo, La Mesa, Ambuklao, Binga, San Roque, Pantabangan, Magat, Caliraya.

Data type: **in-situ** (telemetered / dam-operator gauge, not remote sensing).

## Scripts

- `philippines_pagasa_scraper.py` — daily scraper. Reads the PAGASA flood page, extracts the dam status table, writes per-date snapshot CSVs. Idempotent: re-runs merge rather than overwrite.
- `philippines_pagasa_wayback_backfill.py` — one-off historical restore. Walks Wayback Machine captures of the same URL to reconstruct ~160 scattered days of history (earliest 2021-09). Not scheduled; run manually if the data directory is wiped.

## Variables captured

Per snapshot:
- `Reservoir Water Level (RWL, m)`
- `Water Level Deviation (24hr, m)` — today-only column on the page
- `Normal High Water Level (NHWL, m)`
- `Deviation from NHWL (m)`
- `Rule Curve Elevation (m)`
- `Deviation from Rule Curve (m)`
- `Gate Opening (gates)` / `Gate Opening (meters)`
- `Inflow (cms)` / `Outflow (cms)` — usually blank outside release events

## Env vars

| Var | Purpose |
|---|---|
| `OUTPUT_DIR` | Override output root (default `data/philippines/pagasa`) |
| `SAVE_RAW_HTML` | `0` to skip saving raw HTML |
| `PHILIPPINES_WB_FROM` / `PHILIPPINES_WB_TO` | Date range for Wayback backfill |
| `PHILIPPINES_WB_LIMIT` | Stop after N Wayback snapshots (smoke test) |
| `PHILIPPINES_WB_DELAY` | Seconds between Wayback fetches (default 1.5) |

## Known limits

- PAGASA only publishes the **latest 2 days**; there is no historical API. We rely on daily scheduled runs to accrue a continuous record from install date forward.
- `lat`/`lon`/`capacity_total`/`dam_height`/`year_built`/`main_use` are hardcoded in `DAM_REFERENCE` from public sources (Wikipedia, NPC, NIA, MWSS fact sheets). Verify if precision matters.
- `dead_storage` and `frl` are not published by PAGASA here and are left blank.
- Wayback coverage is sparse (~160 unique days over 4.5 years), concentrated in typhoon season.

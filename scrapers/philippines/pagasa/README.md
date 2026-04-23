# Philippines PAGASA dam scraper

Source: [https://www.pagasa.dost.gov.ph/flood](https://www.pagasa.dost.gov.ph/flood)

Covers 9 major Luzon dams: Angat, Ipo, La Mesa, Ambuklao, Binga, San Roque, Pantabangan, Magat, Caliraya.

Data type: **in-situ** (telemetered / dam-operator gauge, not remote sensing).

## Scripts

- `philippines_pagasa_scraper.py` — daily scraper. Reads the PAGASA flood page, extracts the dam status table, writes per-date snapshot CSVs. Idempotent: re-runs merge rather than overwrite.

A one-off Wayback Machine backfill script exists off-repo (covers ~160 scattered days back to 2021-09) and is kept locally; it is not needed for steady-state daily operation.

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

## Known limits

- PAGASA only publishes the **latest 2 days**; there is no historical API. We rely on daily scheduled runs to accrue a continuous record from install date forward.
- `lat`/`lon`/`capacity_total`/`dam_height`/`year_built`/`main_use` are hardcoded in `DAM_REFERENCE` from public sources (Wikipedia, NPC, NIA, MWSS fact sheets). Verify if precision matters.
- `dead_storage` and `frl` are not published by PAGASA here and are left blank.

# India — APWRIMS

In-situ daily reservoir observations (water level, storage, inflow, outflow,
canal-wise releases) for **118 reservoirs** in Andhra Pradesh, Telangana, and
the wider Krishna basin, from the
[APWRIMS public API](https://apwrims.ap.gov.in/).

Values come from physical level sensors at the dam and operator-reported
gate / spillway / power-house flow logs — not remote sensing.

## Source units (kept raw per `schema.md`)

| Quantity | API field | Stored as |
|---|---|---|
| Water level | `level` | `m` (masl) |
| Storage | `storage` | `TMC` (Thousand Million Cubic feet) |
| Capacity, dead storage | `designcapacity`, `deadstorage` | `TMC` |
| Inflow / outflow | `inflow`, `outflow` | `cusec` (cubic feet per second) |

Conversions to SI happen downstream in the project's normalize step, not here.

## Endpoints

| Endpoint | Returns |
|---|---|
| `GET /api/reservoir/map/all` | List of 118 reservoirs (UUID, name, lat/lon) |
| `GET /api/v2/reservoir/extension/<uuid>` | Static metadata (capacity, FRL, river, basin, districts) |
| `GET /api/v2/reservoir/getlastnvalues/<uuid>/<n>` | Most recent 3-4 observations |

The `getlastnvalues` endpoint is **server-capped at ~3 records** regardless of
the requested `n` — it is a "live status" endpoint, not a historical archive.
The daily cron and the per-date merge logic are designed around this:

1. Daily run fetches the last 3-4 observations per reservoir.
2. Each observation is bucketed by UTC date, latest event per date wins.
3. New rows are merged into per-day CSV files, idempotent on `reservoir_id`.
4. Over time the per-day CSVs accumulate into a continuous daily history
   (~94 reservoirs report each day; ~24 dormant barrages may be empty).

If the cron is paused for more than 3 days, observations from the gap window
are lost. Keep the cron running.

## Running locally

```bash
pip install -r requirements.txt
OUTPUT_DIR=data/india/apwrims python scrapers/india/apwrims/india_apwrims_scraper.py
```

## Cron

`.github/workflows/india_apwrims.yml` runs daily at 03:00 UTC (08:30 IST),
commits any new rows under `data/india/apwrims/`, and pushes back to `main`.

## Why APWRIMS instead of CWC

The official CWC weekly storage bulletin (cwc.gov.in) is geo-blocked at the
WAF level and rejects every request from outside India (including GitHub
Actions runners), so it cannot drive a hosted cron. APWRIMS is the only
Indian state-level water-resources API reachable from US-based runners. CWC
historical PDFs are backfilled separately via the Wayback Machine (a
local-only task, not in this repo).

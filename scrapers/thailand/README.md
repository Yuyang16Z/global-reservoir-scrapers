# Thailand — scrapers overview

## Sub-sources

| Folder | Source | Kind | Records | Schedule | Script |
|---|---|---|---|---|---|
| [`rid/`](./rid/) | RID (Royal Irrigation Department) JSON API | ✅ Reservoir (dynamic) | 35 large + 448 medium | 2×/day cron | `rid/thailand_rid_scraper.py` |

Output goes to `data/thailand/<source>/`.

## `rid/` — notes

- Backing URL: <https://app.rid.go.th/reservoir/>
- Endpoints (POST, form-urlencoded):
  - `https://app.rid.go.th/reservoir/api/dams` — large dams
  - `https://app.rid.go.th/reservoir/api/rsvmiddles` — medium reservoirs (`status=1` = active)
- Update cadence: once per day, usually before 08:00 Bangkok (UTC+7). Cron fires 01:30 + 13:30 UTC as a belt-and-braces pair; each run re-fetches yesterday + today.
- Backfill mode: set `THAILAND_START_DATE` + `THAILAND_END_DATE` env vars, or use the `workflow_dispatch` inputs.
- Units per RID convention — all storage / inflow / outflow volumes are **million m³** (ล้าน ลบ.ม.). Percentage columns are `%` of some reference capacity (see RID for details). **No unit conversion at scrape time.**
- `reservoir_id` is RID's own code (`200101` = Bhumibol, `rsv01` = ..., etc.).
- `source_type=large` means from `/api/dams`, `middle` means from `/api/rsvmiddles`.

## Deferred sources

- **EGAT hydro dams** (Bhumibol / Sirikit / Srinagarind / Vajiralongkorn / Rajjaprabha / Pak Mun / Sirindhorn / Chulabhorn) — EGAT publishes its own operational data, but the key large EGAT dams already appear in RID's `/api/dams` response. No second source needed unless you want sub-daily / generation data.
- **TMD / DWR river-stage** — not reservoir data; falls under the rivers / discharge layer, mirror the Malaysia Sarawak pattern if needed later.
- **Thaiwater.net** — aggregator, mostly republishing RID. Skipped.

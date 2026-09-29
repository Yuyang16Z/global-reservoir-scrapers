# Argentina — CAMMESA daily hydro snapshot

`argentina_cammesa_hidro_scraper.py` archives CAMMESA's daily page
"Datos de Embalses y Centrales Hidráulicas":

```
https://microfe.cammesa.com/static-content/CammesaWeb/download-manager-files/unifilares/hidro/<YYYYMMDD>/www.cammesa.com/OpenDocument.html
```

- **Retention: rolling window, about 32 days** (2026-09-28: earliest page 2026-08-28; 2026-08-27 expired
  that day; 2026-08-31 never published; no Internet Archive copies). Every run re-requests the whole
  window (Argentina today − 40 days … today).
- **Variables per reservoir box:** `level_today_masl` (Cota Hoy), `level_max_masl` / `level_min_masl`
  (operating band printed on the page), `turbined_m3s`, `spilled_m3s`, `pumping_mwh` (Río Grande only).
  River-flow boxes go to a separate CSV.
- **Dates:** `page_date` is the date printed on the page. For Salto Grande the level on page date D equals
  the operator's 24:00 reading of D−1 on 30/30 checked days (value at 00:00 of the page date).
- **Versions:** the CDN injects a random `<script id="f5_cspm">` into every response, so pages are stored once
  per content hash computed without that script. A genuine data change is kept as a new version.
- **Known source quirks:** some boxes repeat the previous day exactly (all four San Juan boxes on 2026-09-15
  and 2026-09-17); Yacyretá 2026-09-07 prints 82.05 between ~82.9 readings (probable typo). Both are kept
  as published.
- **Licence:** CAMMESA's terms prohibit reproduction or distribution without its express authorisation
  (`prohibited`, checked 2026-09-28). Collected here by owner decision of 2026-09-28; see
  `DATA_LICENCE_NOTICE.md`. Not permission to republish.

Outputs under `data/argentina/cammesa_hidro/`: `raw/`, `timeseries/argentina_cammesa_hidro_reservoirs.csv`
(latest version per page date), `..._reservoirs_all_versions.csv`, `..._rivers.csv`, `run_logs/`.
The pages from 2026-08-28 to 2026-09-28 were captured locally on 2026-09-28 before this workflow existed;
`run_logs/2026-09-28T224244Z_local_seed_summary.json` records when each was fetched.

Backfill or recovery: run the workflow manually with `start_date` / `end_date`, or locally:

```bash
OUTPUT_DIR=data/argentina/cammesa_hidro START_DATE=2026-09-01 END_DATE=2026-09-28 \
  python scrapers/argentina/cammesa_hidro/argentina_cammesa_hidro_scraper.py
```

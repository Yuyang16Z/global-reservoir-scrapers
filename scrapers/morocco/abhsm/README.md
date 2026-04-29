# Morocco ABHSM scraper

Official source:
- Current barrage situation PDF: <https://www.abhsm.ma/document/Remplissage_barrage/remplissage_barrage.pdf>

Coverage:
- 9 Souss-Massa dams only
- Source is an official current-state PDF, so this scraper stores:
  - static metadata
  - one daily snapshot CSV
  - the original PDF

## Run locally

```bash
python scrapers/morocco/abhsm/morocco_abhsm_scraper.py
```

Or override output:

```bash
OUTPUT_DIR=/tmp/morocco_abhsm python scrapers/morocco/abhsm/morocco_abhsm_scraper.py
```

## Output layout

```text
<OUTPUT_DIR>/
├── metadata/morocco_abhsm_reservoirs.csv
├── timeseries/daily/morocco_abhsm_timeseries_YYYY-MM-DD.csv
├── raw/2026-04-29_situation_des_barrages_souss_massa.pdf
└── run_logs/YYYYmmdd_HHMMSS_summary.json
```

## Notes

- This is intentionally in the scheduled repo because the public PDF is an ephemeral current snapshot and may be overwritten later.
- It is not a full-country Morocco source. It only covers the Souss-Massa basin authority's 9 dams.

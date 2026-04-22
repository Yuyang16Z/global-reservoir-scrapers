# Global Reservoir Scrapers

Scheduled government/agency reservoir scrapers for the Global Reservoir Dataset project
(PI: Prof. Ximing Cai, UIUC). Raw timeseries feeding a unified global reservoir dataset
targeted for **June 2026**.

Each country's scraper lives under `scrapers/<country>/` and writes outputs into
`data/<country>/` following the conventions in [`schema.md`](./schema.md).

## Layout

```
.
├── schema.md                       # output format convention (follow this for new countries)
├── requirements.txt
├── scrapers/
│   └── malaysia/
│       ├── malaysia_luas_scraper.py
│       └── README.md
├── data/                           # populated by scheduled workflows, committed back
│   └── malaysia/
│       ├── metadata/
│       ├── timeseries/daily/
│       ├── raw/
│       └── run_logs/
└── .github/workflows/
    └── malaysia_luas.yml           # cron 02:00 + 14:00 UTC
```

## Current coverage

### Reservoir data

| Country | Source | Cadence | Script | Status |
|---|---|---|---|---|
| Malaysia (Selangor) | LUAS IWRIMS JSON API (8 dams + 1 barrage) | daily snapshot, 2× per day | `scrapers/malaysia/malaysia_luas_scraper.py` | ✅ v1 (2026-04-21) |
| Malaysia (nationwide) | MyWater Portal — JPS dams (16 static metadata) | **manual trigger only** (source static) | `scrapers/malaysia_mywater/mywater_jps_scraper.py` | ✅ v1 (2026-04-22) |

### River / rainfall discharge layer (NOT reservoir data — cross-reference only)

| Country | Source | Cadence | Script | Status |
|---|---|---|---|---|
| Malaysia (Sarawak) | DID Sarawak iHydro (~269 river + rainfall + IG stations) | 2× per day | `scrapers/malaysia_sarawak_rivers/sarawak_ihydro_scraper.py` | ✅ v1 (2026-04-22) |

Other countries (Argentina, Australia, China, India, Taiwan, Thailand, South Africa,
Zambia, Central Asia, etc.) are scraped locally from `~/Desktop/work/resovoir data/`
and are not yet migrated here.

## Running locally

```bash
pip install -r requirements.txt
python scrapers/malaysia/malaysia_luas_scraper.py
# → writes to ./scrapers/malaysia/malaysia_luas_outputs/

# or pin a custom output dir:
OUTPUT_DIR=/tmp/luas python scrapers/malaysia/malaysia_luas_scraper.py
```

## Scheduled runs

GitHub Actions runs each country's workflow on a cron and commits new data
back to the repo. See `.github/workflows/` for the exact schedules.
GitHub cron is best-effort — actual fire time can lag 5–30 min during load.

# Global Reservoir Scrapers

Scheduled government/agency reservoir scrapers for the Global Reservoir Dataset project
(PI: Prof. Ximing Cai, UIUC). Raw timeseries feeding a unified global reservoir dataset
targeted for **June 2026**.

Each country lives under `scrapers/<country>/` and writes outputs into the mirror path
`data/<country>/`. Countries with multiple data sources (e.g. Malaysia) have per-source
subfolders. Output formats follow [`schema.md`](./schema.md).

## Layout

```
.
├── schema.md                       # output format convention (follow this for new countries)
├── requirements.txt
├── scrapers/
│   ├── malaysia/
│   │   ├── README.md               # overview of all Malaysia sub-sources
│   │   ├── luas/                   # LUAS IWRIMS (Selangor reservoirs)
│   │   ├── mywater/                # MyWater JPS dams (nationwide, static metadata)
│   │   └── sarawak_rivers/         # DID Sarawak iHydro (rivers + rainfall, NOT reservoirs)
│   └── thailand/
│       ├── README.md
│       └── rid/                    # RID (Royal Irrigation Dept) — 35 large + 448 medium
├── data/                           # populated by scheduled workflows, committed back
│   ├── malaysia/{luas,mywater,sarawak_rivers}/
│   └── thailand/rid/
└── .github/workflows/
    ├── malaysia_luas.yml           # cron 02:00 + 14:00 UTC
    ├── malaysia_mywater.yml        # manual trigger only
    ├── malaysia_sarawak_rivers.yml # cron 02:05 + 14:05 UTC
    └── thailand_rid.yml            # cron 01:30 + 13:30 UTC
```

## Current coverage

### Reservoir data

| Country | Source | Cadence | Script | Status |
|---|---|---|---|---|
| Malaysia (Selangor) | LUAS IWRIMS JSON API (8 dams + 1 barrage) | daily snapshot, 2× per day | `scrapers/malaysia/luas/malaysia_luas_scraper.py` | ✅ v1 (2026-04-21) |
| Malaysia (nationwide) | MyWater Portal — JPS dams (16 static metadata) | **manual trigger only** (source static) | `scrapers/malaysia/mywater/mywater_jps_scraper.py` | ✅ v1 (2026-04-22) |
| Thailand (nationwide) | RID Royal Irrigation Dept JSON API (35 large + 448 medium) | daily snapshot, 2× per day | `scrapers/thailand/rid/thailand_rid_scraper.py` | ✅ v1 (2026-04-22) |

### River / rainfall discharge layer (NOT reservoir data — cross-reference only)

| Country | Source | Cadence | Script | Status |
|---|---|---|---|---|
| Malaysia (Sarawak) | DID Sarawak iHydro (~269 river + rainfall + IG stations) | 2× per day | `scrapers/malaysia/sarawak_rivers/sarawak_ihydro_scraper.py` | ✅ v1 (2026-04-22) |

Other countries (Argentina, Australia, China, India, Taiwan, Thailand, South Africa,
Zambia, Central Asia, etc.) are scraped locally from `~/Desktop/work/resovoir data/`
and are not yet migrated here.

## Running locally

```bash
pip install -r requirements.txt

# LUAS (Selangor reservoirs)
python scrapers/malaysia/luas/malaysia_luas_scraper.py

# MyWater JPS dams metadata
python scrapers/malaysia/mywater/mywater_jps_scraper.py

# Sarawak river gauges
python scrapers/malaysia/sarawak_rivers/sarawak_ihydro_scraper.py

# Thailand RID reservoirs
python scrapers/thailand/rid/thailand_rid_scraper.py

# Output dir defaults to the scraper's folder; override with OUTPUT_DIR:
OUTPUT_DIR=/tmp/luas python scrapers/malaysia/luas/malaysia_luas_scraper.py
```

## Scheduled runs

GitHub Actions runs each country's workflow on a cron and commits new data
back to the repo. See `.github/workflows/` for the exact schedules.
GitHub cron is best-effort — actual fire time can lag 5–30 min during load.

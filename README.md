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
│   ├── china/
│   │   ├── mwr/                    # legacy Selenium + OCR archive path
│   │   └── mwr_api/                # MWR realtime table API + font decoder
│   ├── india/
│   │   └── apwrims/                # APWRIMS recent reservoir observations
│   ├── japan/
│   │   └── opengov/                # OpenGov / MLIT dam reservoir pages
│   ├── luxembourg/
│   │   └── age/                     # AGE Haute-Sure reservoir level
│   ├── malaysia/
│   │   ├── README.md               # overview of all Malaysia sub-sources
│   │   ├── luas/                   # LUAS IWRIMS (Selangor reservoirs)
│   │   ├── mywater/                # MyWater JPS dams (nationwide, static metadata)
│   │   └── sarawak_rivers/         # DID Sarawak iHydro (rivers + rainfall, NOT reservoirs)
│   ├── morocco/
│   │   ├── README.md
│   │   └── abhsm/                  # ABHSM Souss-Massa daily barrage PDF
│   ├── southafrica/
│   │   └── dws_weekly/             # DWS Weekly State of the Reservoirs bulletin
│   ├── taiwan/
│   │   ├── README.md
│   │   └── wra/                    # WRA open data + disaster-prevention APIs
│   └── thailand/
│       ├── README.md
│       └── rid/                    # RID (Royal Irrigation Dept) — 35 large + 448 medium
├── data/                           # populated by scheduled workflows, committed back
│   ├── china/{mwr,mwr_api}/
│   ├── india/apwrims/
│   ├── japan/opengov/
│   ├── luxembourg/age/
│   ├── malaysia/{luas,mywater,sarawak_rivers}/
│   ├── morocco/abhsm/
│   ├── southafrica/dws_weekly/
│   ├── taiwan/wra/
│   └── thailand/rid/
└── .github/workflows/
    ├── china_mwr_api.yml           # cron 12:30 UTC
    ├── india_apwrims.yml           # cron 03:00 + 12:00 UTC
    ├── japan_opengov.yml           # cron 01:20 UTC
    ├── luxembourg_age.yml          # cron 04:17 + 16:17 UTC
    ├── malaysia_luas.yml           # cron 02:00 + 14:00 UTC
    ├── malaysia_mywater.yml        # manual trigger only
    ├── malaysia_sarawak_rivers.yml # cron 02:05 + 14:05 UTC
    ├── morocco_abhsm.yml           # cron 07:30 + 15:30 UTC
    ├── southafrica_dws_weekly.yml  # cron 06:00 UTC every Tuesday
    ├── taiwan_wra.yml              # cron 01:45 UTC Mondays
    └── thailand_rid.yml            # cron 01:30 UTC Mondays
```

## Current coverage

### Reservoir data

| Country | Source | Cadence | Script | Status |
|---|---|---|---|---|
| China (nationwide) | MWR 全国大型水库实时水情 API + font decoder (~570 reservoirs) | daily snapshot, 1× per day | `scrapers/china/mwr_api/china_mwr_api_scraper.py` | ✅ v1 (2026-06-29) |
| India (AP / Telangana / Krishna basin) | APWRIMS public API (118 reservoirs) | recent-observation window, 2× per day | `scrapers/india/apwrims/india_apwrims_scraper.py` | ✅ v1 (2026-06-19) |
| Japan (nationwide) | OpenGov / MLIT dam reservoir pages | daily scrape, 1× per day | `scrapers/japan/opengov/japan_opengov_scraper.py` | ✅ v1 (2026-04-22) |
| Luxembourg (Haute-Sure) | AGE station 40 graph API | rolling five-day window, 2× per day | `scrapers/luxembourg/age/luxembourg_age_scraper.py` | ✅ v1 (2026-07-21) |
| Malaysia (Selangor) | LUAS IWRIMS JSON API (8 dams + 1 barrage) | daily snapshot, 2× per day | `scrapers/malaysia/luas/malaysia_luas_scraper.py` | ✅ v1 (2026-04-21) |
| Malaysia (nationwide) | MyWater Portal — JPS dams (16 static metadata) | **manual trigger only** (source static) | `scrapers/malaysia/mywater/mywater_jps_scraper.py` | ✅ v1 (2026-04-22) |
| Morocco (Souss-Massa) | ABHSM daily barrage situation PDF (9 dams) | daily snapshot + raw PDF, 2× per day | `scrapers/morocco/abhsm/morocco_abhsm_scraper.py` | ✅ v1 (2026-04-29) |
| South Africa (nationwide) | DWS Weekly State of the Reservoirs PDF (~222 reservoirs) | weekly snapshot, every Tuesday | `scrapers/southafrica/dws_weekly/dws_weekly_scraper.py` | ✅ v1 (2026-04-28) |
| Taiwan (nationwide) | WRA open data + disaster-prevention APIs | weekly run, rolling 15-day backfill | `scrapers/taiwan/wra/taiwan_wra_scraper.py` | ✅ v1 (2026-04-22) |
| Thailand (nationwide) | RID Royal Irrigation Dept JSON API (35 large + 448 medium) | weekly run, rolling 15-day backfill | `scrapers/thailand/rid/thailand_rid_scraper.py` | ✅ v1 (2026-04-22) |

### River / rainfall discharge layer (NOT reservoir data — cross-reference only)

| Country | Source | Cadence | Script | Status |
|---|---|---|---|---|
| Malaysia (Sarawak) | DID Sarawak iHydro (~269 river + rainfall + IG stations) | 2× per day | `scrapers/malaysia/sarawak_rivers/sarawak_ihydro_scraper.py` | ✅ v1 (2026-04-22) |

Other countries (Argentina, Australia, Zambia, Central Asia, etc.)
are scraped locally from `~/Desktop/work/resovoir data/` and are not yet
migrated here.

## Running locally

```bash
pip install -r requirements.txt

# LUAS (Selangor reservoirs)
python scrapers/malaysia/luas/malaysia_luas_scraper.py

# China MWR large reservoirs
python scrapers/china/mwr_api/china_mwr_api_scraper.py

# India APWRIMS reservoirs
python scrapers/india/apwrims/india_apwrims_scraper.py

# Japan OpenGov reservoirs
python scrapers/japan/opengov/japan_opengov_scraper.py

# Luxembourg AGE Haute-Sure reservoir level
python scrapers/luxembourg/age/luxembourg_age_scraper.py

# MyWater JPS dams metadata
python scrapers/malaysia/mywater/mywater_jps_scraper.py

# Sarawak river gauges
python scrapers/malaysia/sarawak_rivers/sarawak_ihydro_scraper.py

# Morocco ABHSM current barrage PDF
python scrapers/morocco/abhsm/morocco_abhsm_scraper.py

# South Africa DWS weekly bulletin
python scrapers/southafrica/dws_weekly/dws_weekly_scraper.py

# Taiwan WRA reservoirs
python scrapers/taiwan/wra/taiwan_wra_scraper.py

# Thailand RID reservoirs
python scrapers/thailand/rid/thailand_rid_scraper.py

# Output dir defaults to the scraper's folder; override with OUTPUT_DIR:
OUTPUT_DIR=/tmp/luas python scrapers/malaysia/luas/malaysia_luas_scraper.py
```

## Scheduled runs

GitHub Actions runs each country's workflow on a cron and commits new data
back to the repo. See `.github/workflows/` for the exact schedules.
GitHub cron is best-effort — actual fire time can lag 5–30 min during load.

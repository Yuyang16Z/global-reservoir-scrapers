# Malaysia — MyWater Portal (JPS dams) metadata

## What this covers
- **16 dams** operated by JPS (Jabatan Pengairan dan Saliran / DID Malaysia)
- **Static attributes only** (dam height, crest length, crest elevation, catchment, capacity, normal level)
- **No timeseries** — source doesn't publish one
- Source page `last updated 2025-05-30` per the portal footer

## Why in the repo
Even without timeseries, this is the only single-page inventory of all national JPS dams with consistent attributes. It's useful as a **master reservoir inventory** to cross-reference against live sources (Public Infobanjir, state portals) and against the Global Dam Watch v1.0 dataset at `~/Desktop/work/GDW data/`.

## Schedule
**Manual trigger only** (`workflow_dispatch`). The source barely changes — there's no point polling it. Run it once a year or when JPS announces a new dam.

## Duplicate alert
`BATU` here is the **same physical dam** as `BATU` in the LUAS IWRIMS source (`MY_LUAS_1304`). Different `reservoir_id`, since they come from different agencies with different data. Downstream normalize step should merge them when the master cross-source reservoir map is built.

## Missing fields (intentionally blank)
- `river`, `basin`, `year_built` — source doesn't provide
- `lat`, `lon` — source doesn't provide; cross-ref with GDW v1.0 or OpenStreetMap in a future step

## Running locally
```bash
cd "/Users/andy/Desktop/work/global-reservoir-scrapers"
python scrapers/malaysia_mywater/mywater_jps_scraper.py
```

## Dam list
ANAK ENDAU (Pahang), BATU (Selangor), BEKOK (Johor), BERIS (Kedah), BUKIT KWONG (Kelantan),
BUKIT MERAH (Perak), GOPENG (Perak), LABONG (Johor), MACHAP (Johor), PADANG SAGA (Kedah),
PERTING (Pahang), PONTIAN (Pahang), REPAS BARU (Pahang), REPAS LAMA (Pahang), SEMBRONG (Johor),
TIMAH TASOH (Perlis).

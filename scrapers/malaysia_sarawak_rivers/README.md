# Malaysia — Sarawak iHydro (RIVERS + rainfall, not reservoirs)

## ⚠️ Not reservoir data

These are **river / rainfall / IG gauges** operated by DID Sarawak. They are **not reservoir stations**.

Sarawak's big hydroelectric reservoirs (**Bakun / Murum / Batang Ai**) are operated by
**Sarawak Energy (SEB)** and do **not publish live operational data** publicly — only press
releases on sarawakenergy.com. This scraper cannot reach those. Do not expect reservoir
water level or storage here.

Why it lives in this repo anyway: river stage + basin rainfall is a **discharge/forcing layer**
for the dataset. It's useful for cross-referencing inflow/outflow of reservoirs in other sources
that sit on the same river basins.

## What the scraper captures (~271 stations, ~269 usable)

- **Station metadata**: station_id, station_name, **station_type** (Rainfall / Combine / Water / IG),
  division, river basin, lat, lon, alert / warning / danger / normal level thresholds, WL datum.
- **Latest reading snapshot**: water level (m), WL status, daily rainfall (mm), latest rainfall (mm), rainfall status, observation timestamp.

"Combine" stations have both WL and rainfall; "Water" only WL; "Rainfall" only rainfall; "IG" are Intelligent Gauges (early-warning).

## Source & request pattern

One single HTTP GET to `https://ihydro.sarawak.gov.my/iHydro/en/map/maps.jsp` yields
the entire network in a hidden `<input id="xml">` JS-array. We prefer this over the
paginated `latest-waterlevel.jsp` because it has more fields (incl. lat/lon) in 1 request.

## Schedule

Cron: **02:00 + 14:00 UTC** (= 10:00 + 22:00 Malaysia). Source refreshes roughly every 15 min,
two snapshots per day is enough to build a series.

## Running locally

```bash
cd "/Users/andy/Desktop/work/global-reservoir-scrapers"
python scrapers/malaysia_sarawak_rivers/sarawak_ihydro_scraper.py
```

## Output layout

```
data/malaysia_sarawak_rivers/
├── metadata/malaysia_sarawak_rivers_stations.csv
├── timeseries/daily/malaysia_sarawak_rivers_timeseries_YYYY-MM-DD.csv
├── raw/maps_YYYYMMDD_HHMMSS.html
└── run_logs/<stamp>_summary.json
```

Column key: `station_id` (not `reservoir_id`) — this is intentional; these are stations, not reservoirs.

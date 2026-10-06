# Taiwan WRA scraper

API-first scraper for Taiwan reservoir data from the Water Resources Agency (WRA).

## Sources

- Current daily operation status:
  `https://opendata.wra.gov.tw/api/v2/51023e88-4c76-4dbc-bbb9-470da690d539?format=JSON&sort=_importdate+asc`
- Current water level dataset:
  `https://opendata.wra.gov.tw/api/v2/2be9044c-6e44-4856-aad5-dd108c2e6679?format=JSON&sort=_importdate+asc`
- Annual reservoir basic information:
  `https://opendata.wra.gov.tw/api/v2/708a43b0-24dc-40b7-9ed2-fca6a291e7ae?format=JSON&sort=_importdate+asc`
- Retired: the keyless history API
  `https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date=YYYY-MM-DD` has
  answered "HTTP Error 503. The service is unavailable." on every path since
  2026-06-13. Its successor, FHY General API v2
  (`https://fhy.wra.gov.tw/Api/v2/Reservoir/Daily`, header `apikey`), needs a
  key that WRA issues to government bodies; its documentation sends other
  users to the open-data platform. No source this scraper may use can supply
  a past day.
- Static lat/lon lookup:
  `reservoir_coords.csv` (one-time extract, centroid of reservoir storage-area
  polygons from `gic.wra.gov.tw` SHP: `ressub` with `reservoir` as fallback,
  reprojected TWD97 TM2 → WGS84). Regenerate by downloading
  `DownLoad.aspx?fname=ressub&filetype=SHP` + `fname=RESERVOIR&filetype=SHP`
  and running the centroid + name-alias matcher.

## What it writes

- `metadata/taiwan_wra_reservoirs.csv`
- `timeseries/daily/taiwan_timeseries_YYYY-MM-DD.csv`
- `timeseries/intraday/taiwan_intraday_YYYY-MM-DD.csv`
- `raw/static_reservoirs.json`
- `raw/current_daily_ops_YYYY-MM-DD.json`, `raw/current_water_level_YYYY-MM-DD.json`
  (named after the run's Taiwan date)
- `raw/daily/YYYY-MM-DD.json` (history API responses up to 2026-06-11)
- `run_logs/<timestamp>_summary.json`

## Notes

- Each run files the official daily-operations snapshot under the date the
  source reports (normally yesterday in Taiwan time). A table already archived
  for that date is left unchanged.
- The snapshot holds one day and is replaced once a day, so a day is archived
  only if a run lands while it is current. The workflow runs twice a day,
  12 hours apart, for that reason. When a run finds days missing between the
  newest earlier table and the snapshot, it lists them as `missed_dates` in its
  run summary and raises a workflow warning; those days cannot be fetched
  later.
- `status` in the run summary is `ok` when a snapshot was filed or was already
  archived, and `source_unavailable` when the dataset gave no usable snapshot.
- Existing `raw/current_daily_ops_*.json` snapshots are used to recover missing
  daily CSVs under their dominant source-reported date. This recovers only
  observations actually archived by the project and does not invent gaps.
- The current water-level dataset is also written out as an intraday table so the
  hourly observations are preserved instead of only keeping one latest snapshot per reservoir.
- `lat` / `lon` are populated from `reservoir_coords.csv`, a static lookup derived
  from the WRA GIS `ressub` (storage-area) and `reservoir` (catchment) shapefiles.
  72 of 74 reservoirs currently resolve; the remaining 2 (珠螺水壩, 儲水沃上壩 in
  Matsu) aren't in those SHPs — left blank rather than guessed.
- `dam_type` / `dam_height` / `dam_length` / `catchment_area` / `surface_area_frl` /
  `capacity_design_*` / `capacity_current_*` / `main_use` / `operator` /
  `last_capacity_survey_year_roc` come from the `basic_info` endpoint. Only 40 of
  the 74 reservoirs (the major ones tracked by WRA headquarters) have these fields;
  離島 reservoirs in 金門/馬祖/澎湖 are absent from that endpoint.

## Run locally

```bash
python scrapers/taiwan/wra/taiwan_wra_scraper.py
```

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `OUTPUT_DIR` | `./taiwan_wra_outputs` next to the script | Where outputs are written |
| `SAVE_RAW_JSON` | `1` | Save raw JSON payloads for audit/debug |

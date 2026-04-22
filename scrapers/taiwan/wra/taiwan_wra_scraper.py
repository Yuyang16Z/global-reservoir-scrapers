"""Taiwan WRA reservoir scraper.

Sources:
- Static name mapping:
  https://data.wra.gov.tw/Service/OpenData.aspx?format=json&id=E2_7_00001
- Daily operations:
  https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date=YYYY-MM-DD
- Current realtime snapshot:
  https://fhy.wra.gov.tw/WraApi/v1/Reservoir/RealTime

Default behavior:
- Fetch yesterday + today in Taiwan time (UTC+8)
- Write one daily snapshot CSV per date under timeseries/daily/
- Keep metadata separate from daily observations
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


HIST_DAILY_URL = "https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date={date}"
CURRENT_DAILY_OPS_URL = (
    "https://opendata.wra.gov.tw/api/v2/51023e88-4c76-4dbc-bbb9-470da690d539"
    "?format=JSON&sort=_importdate+asc"
)
CURRENT_WATER_LEVEL_URL = (
    "https://opendata.wra.gov.tw/api/v2/2be9044c-6e44-4856-aad5-dd108c2e6679"
    "?format=JSON&sort=_importdate+asc"
)
BASIC_INFO_URL = (
    "https://opendata.wra.gov.tw/api/v2/708a43b0-24dc-40b7-9ed2-fca6a291e7ae"
    "?format=JSON&sort=_importdate+asc"
)
SOURCE_URL = "https://data.gov.tw/en/datasets/45501"
SOURCE_AGENCY = "WRA"
TAIWAN_TZ = timezone(timedelta(hours=8))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

TS_COLUMNS: list[tuple[str, str]] = [
    ("reservoir_id", "reservoir_id"),
    ("reservoir_name", "reservoir_name"),
    ("date", "date"),
    ("ObservationTime", "observation_time"),
    ("WaterLevel (m)", "water_level_m"),
    ("PercentageStorage (%)", "percentage_storage_pct"),
    ("EffectiveWaterStorageCapacity (10^4 m^3)", "effective_storage_capacity_10k_m3"),
    ("AccumulateRainfallInCatchment (mm)", "rainfall_in_catchment_mm"),
    ("InflowTotal (10^4 m^3)", "inflow_total_10k_m3"),
    ("OutflowTotal (10^4 m^3)", "outflow_total_10k_m3"),
    ("WaterDraw (10^4 m^3)", "water_draw_10k_m3"),
    ("PredeterminedCrossFlow (10^4 m^3)", "predetermined_crossflow_10k_m3"),
    ("DesiltingTunnelOutflow (10^4 m^3)", "desilting_tunnel_outflow_10k_m3"),
    ("DrainageTunnelOutflow (10^4 m^3)", "drainage_tunnel_outflow_10k_m3"),
    ("PowerOutletOutflow (10^4 m^3)", "power_outlet_outflow_10k_m3"),
    ("SpillwayOutflow (10^4 m^3)", "spillway_outflow_10k_m3"),
    ("OthersOutflow (10^4 m^3)", "others_outflow_10k_m3"),
    ("StatusType", "status_type"),
]

INTRADAY_COLUMNS: list[tuple[str, str]] = [
    ("reservoir_id", "reservoir_id"),
    ("reservoir_name", "reservoir_name"),
    ("date", "date"),
    ("ObservationTime", "observation_time"),
    ("WaterLevel (m)", "water_level_m"),
    ("EffectiveWaterStorageCapacity (10^4 m^3)", "effective_storage_capacity_10k_m3"),
    ("AccumulateRainfallInCatchment (mm)", "rainfall_in_catchment_mm"),
    ("InflowDischarge (10^4 m^3)", "inflow_discharge_10k_m3"),
    ("TotalOutflow (10^4 m^3)", "outflow_total_10k_m3"),
    ("WaterDraw (10^4 m^3)", "water_draw_10k_m3"),
    ("PredeterminedCrossFlow (10^4 m^3)", "predetermined_crossflow_10k_m3"),
    ("DesiltingTunnelOutflow (10^4 m^3)", "desilting_tunnel_outflow_10k_m3"),
    ("DrainageTunnelOutflow (10^4 m^3)", "drainage_tunnel_outflow_10k_m3"),
    ("PowerOutletOutflow (10^4 m^3)", "power_outlet_outflow_10k_m3"),
    ("SpillwayOutflow (10^4 m^3)", "spillway_outflow_10k_m3"),
    ("OthersOutflow (10^4 m^3)", "others_outflow_10k_m3"),
    ("StatusType", "status_type"),
]

META_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "reservoir_name_en",
    "country",
    "admin_unit",
    "river",
    "basin",
    "lat",
    "lon",
    "source_system",
    "source_agency",
    "source_url",
    "last_updated",
]


def load_manual_overrides(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rid = clean_value(row.get("reservoir_id"))
            if not rid:
                continue
            out[str(rid)] = {
                "reservoir_id": str(rid),
                "reservoir_name": clean_value(row.get("reservoir_name")) or "",
                "admin_unit": clean_value(row.get("admin_unit")) or "",
                "river": clean_value(row.get("river")) or "",
                "basin": clean_value(row.get("basin")) or "",
                "source_system": clean_value(row.get("source_note")) or "Manual override",
            }
    return out


def best_name(
    rid: str,
    direct_name: str | None,
    current_daily_ops_map: dict[str, dict],
    basic_info_map: dict[str, dict],
    manual_overrides: dict[str, dict],
) -> str:
    return (
        clean_value(direct_name)
        or current_daily_ops_map.get(rid, {}).get("reservoir_name")
        or basic_info_map.get(rid, {}).get("reservoir_name")
        or manual_overrides.get(rid, {}).get("reservoir_name")
        or ""
    )


def clean_value(v: Any) -> Any:
    if isinstance(v, str):
        v = v.strip()
        if v in {"", "-", "--", "null", "None"}:
            return None
        return v
    return v


def try_float(v: Any) -> Any:
    v = clean_value(v)
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return v


def get_json(session: requests.Session, url: str, timeout: int = 60) -> Any:
    resp = session.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def target_dates() -> list[str]:
    start = os.environ.get("TAIWAN_START_DATE")
    end = os.environ.get("TAIWAN_END_DATE")
    today = datetime.now(TAIWAN_TZ).date()

    if start or end:
        start_d = parse_date(start or end)
        end_d = parse_date(end or start)
    else:
        start_d = today - timedelta(days=1)
        end_d = today

    if end_d < start_d:
        raise ValueError("TAIWAN_END_DATE must be >= TAIWAN_START_DATE")

    out: list[str] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def ensure_dirs(base: Path) -> dict[str, Path]:
    dirs = {
        "metadata": base / "metadata",
        "daily": base / "timeseries" / "daily",
        "intraday": base / "timeseries" / "intraday",
        "raw": base / "raw",
        "raw_daily": base / "raw" / "daily",
        "logs": base / "run_logs",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_id(row: dict) -> str | None:
    for key in (
        "ReservoirIdentifier",
        "reservoiridentifier",
        "StationNo",
        "stationno",
        "水庫代碼",
        "水库代碼",
        "水库代码",
    ):
        v = clean_value(row.get(key))
        if v is not None:
            return str(v)
    return None


def get_name(row: dict) -> str | None:
    for key in ("ReservoirName", "reservoirname", "水庫名稱", "水库名稱", "水库名称", "Reservoir", "Name"):
        v = clean_value(row.get(key))
        if v is not None:
            return str(v)
    return None


def normalize_basic_info(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        out[rid] = {
            "reservoir_id": rid,
            "reservoir_name": get_name(row),
            "admin_unit": clean_value(row.get("TownName") or row.get("townname") or row.get("鄉鎮市區名稱")),
            "river": clean_value(row.get("RiverName") or row.get("rivername") or row.get("河川名稱")),
            "basin": clean_value(row.get("地區別") or row.get("Area") or row.get("area")),
            "source_system": "opendata.wra.gov.tw Basic Information",
        }
    return out


def normalize_current_water_level(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        record = {
            "reservoir_id": rid,
            "observation_time": clean_value(row.get("ObservationTime") or row.get("observationtime")),
            "water_level_m": try_float(row.get("WaterLevel") or row.get("waterlevel")),
            "percentage_storage_pct": try_float(row.get("PercentageStorage") or row.get("percentagestorage")),
            "effective_storage_capacity_10k_m3": try_float(
                row.get("EffectiveWaterStorageCapacity") or row.get("effectivewaterstoragecapacity")
            ),
            "rainfall_in_catchment_mm": try_float(
                row.get("AccumulateRainfallInCatchment") or row.get("accumulaterainfallincatchment")
            ),
            "water_draw_10k_m3": try_float(row.get("WaterDraw") or row.get("waterdraw")),
            "predetermined_crossflow_10k_m3": try_float(
                row.get("PredeterminedCrossFlow") or row.get("predeterminedcrossflow")
            ),
            "desilting_tunnel_outflow_10k_m3": try_float(
                row.get("DesiltingTunnelOutflow") or row.get("desiltingtunneloutflow")
            ),
            "drainage_tunnel_outflow_10k_m3": try_float(
                row.get("DrainageTunnelOutflow") or row.get("drainagetunneloutflow")
            ),
            "power_outlet_outflow_10k_m3": try_float(
                row.get("PowerOutletOutflow") or row.get("poweroutletoutflow")
            ),
            "spillway_outflow_10k_m3": try_float(row.get("SpillwayOutflow") or row.get("spillwayoutflow")),
            "others_outflow_10k_m3": try_float(row.get("OthersOutflow") or row.get("othersoutflow")),
            "status_type": clean_value(row.get("StatusType") or row.get("statustype")),
            "outflow_total_10k_m3": try_float(row.get("TotalOutflow") or row.get("totaloutflow")),
        }
        prev = out.get(rid)
        if prev is None or (record["observation_time"] or "") >= (prev.get("observation_time") or ""):
            out[rid] = record
    return out


def normalize_current_water_level_intraday(
    rows: list[dict],
    basic_info_map: dict[str, dict],
    current_daily_ops_map: dict[str, dict],
    manual_overrides: dict[str, dict],
) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        obs_time = clean_value(row.get("ObservationTime") or row.get("observationtime"))
        date = ""
        if obs_time:
            date = str(obs_time)[:10]
        out.append({
            "reservoir_id": rid,
            "reservoir_name": best_name(rid, get_name(row), current_daily_ops_map, basic_info_map, manual_overrides),
            "date": date,
            "observation_time": obs_time,
            "water_level_m": try_float(row.get("WaterLevel") or row.get("waterlevel")),
            "effective_storage_capacity_10k_m3": try_float(
                row.get("EffectiveWaterStorageCapacity") or row.get("effectivewaterstoragecapacity")
            ),
            "rainfall_in_catchment_mm": try_float(
                row.get("AccumulateRainfallInCatchment") or row.get("accumulaterainfallincatchment")
            ),
            "inflow_discharge_10k_m3": try_float(row.get("InflowDischarge") or row.get("inflowdischarge")),
            "outflow_total_10k_m3": try_float(row.get("TotalOutflow") or row.get("totaloutflow")),
            "water_draw_10k_m3": try_float(row.get("WaterDraw") or row.get("waterdraw")),
            "predetermined_crossflow_10k_m3": try_float(
                row.get("PredeterminedCrossFlow") or row.get("predeterminedcrossflow")
            ),
            "desilting_tunnel_outflow_10k_m3": try_float(
                row.get("DesiltingTunnelOutflow") or row.get("desiltingtunneloutflow")
            ),
            "drainage_tunnel_outflow_10k_m3": try_float(
                row.get("DrainageTunnelOutflow") or row.get("drainagetunneloutflow")
            ),
            "power_outlet_outflow_10k_m3": try_float(
                row.get("PowerOutletOutflow") or row.get("poweroutletoutflow")
            ),
            "spillway_outflow_10k_m3": try_float(row.get("SpillwayOutflow") or row.get("spillwayoutflow")),
            "others_outflow_10k_m3": try_float(row.get("OthersOutflow") or row.get("othersoutflow")),
            "status_type": clean_value(row.get("StatusType") or row.get("statustype")),
        })
    out.sort(key=lambda r: (r["date"], r["reservoir_id"], r["observation_time"] or ""))
    return out


def normalize_current_daily_ops(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        out[rid] = {
            "reservoir_id": rid,
            "reservoir_name": get_name(row),
            "observation_time": clean_value(row.get("DateTime") or row.get("datetime")),
            "effective_storage_capacity_10k_m3": try_float(row.get("Capacity") or row.get("capacity")),
            "rainfall_in_catchment_mm": try_float(row.get("BasinRainfall") or row.get("basinrainfall")),
            "inflow_total_10k_m3": try_float(row.get("Inflow") or row.get("inflow")),
            "outflow_total_10k_m3": try_float(row.get("OutflowTotal") or row.get("outflowtotal")),
            "water_draw_10k_m3": try_float(row.get("Outflow") or row.get("outflow")),
            "predetermined_crossflow_10k_m3": try_float(row.get("CrossFlow") or row.get("crossflow")),
            "power_outlet_outflow_10k_m3": try_float(
                row.get("RegulatoryDischarge") or row.get("regulatorydischarge")
            ),
            "spillway_outflow_10k_m3": try_float(
                row.get("OutflowDischarge") or row.get("outflowdischarge")
            ),
        }
    return out


def normalize_daily(rows: list[dict], target_date: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        out[rid] = {
            "reservoir_id": rid,
            "reservoir_name": get_name(row),
            "date": target_date,
            "observation_time": clean_value(row.get("Time") or row.get("DateTime") or row.get("ObservationTime")),
            "effective_storage_capacity_10k_m3": try_float(
                row.get("EffectiveCapacity") or row.get("Capacity") or row.get("effectivewaterstoragecapacity")
            ),
            "rainfall_in_catchment_mm": try_float(
                row.get("AccumulatedRainfall") or row.get("BasinRainfall") or row.get("AccumulateRainfallInCatchment")
            ),
            "inflow_total_10k_m3": try_float(row.get("InflowTotal") or row.get("Inflow") or row.get("inflowdischarge")),
            "outflow_total_10k_m3": try_float(
                row.get("OutflowTotal") or row.get("TotalOutflow") or row.get("Outflow") or row.get("totaloutflow")
            ),
            "water_draw_10k_m3": try_float(row.get("WaterDraw") or row.get("waterdraw")),
            "predetermined_crossflow_10k_m3": try_float(
                row.get("CrossFlow") or row.get("PredeterminedCrossFlow") or row.get("predeterminedcrossflow")
            ),
            "desilting_tunnel_outflow_10k_m3": try_float(
                row.get("DesiltingTunnelOutflow") or row.get("desiltingtunneloutflow")
            ),
            "drainage_tunnel_outflow_10k_m3": try_float(
                row.get("DrainageTunnelOutflow") or row.get("drainagetunneloutflow")
            ),
            "power_outlet_outflow_10k_m3": try_float(
                row.get("PowerOutletOutflow") or row.get("poweroutletoutflow")
            ),
            "spillway_outflow_10k_m3": try_float(row.get("SpillwayOutflow") or row.get("spillwayoutflow")),
            "others_outflow_10k_m3": try_float(row.get("OthersOutflow") or row.get("othersoutflow")),
            "status_type": clean_value(row.get("StatusType") or row.get("statustype")),
        }
    return out


def build_rows(
    date_str: str,
    basic_info_map: dict[str, dict],
    current_daily_ops_map: dict[str, dict],
    daily_map: dict[str, dict],
    current_water_level_map: dict[str, dict],
    manual_overrides: dict[str, dict],
    today_tw: str,
) -> list[dict]:
    ids = set(basic_info_map) | set(current_daily_ops_map) | set(daily_map) | set(manual_overrides)
    if date_str == today_tw:
        ids |= set(current_water_level_map)

    rows: list[dict] = []
    for rid in sorted(ids):
        b = basic_info_map.get(rid, {})
        c = current_daily_ops_map.get(rid, {})
        d = daily_map.get(rid, {})
        r = current_water_level_map.get(rid, {}) if date_str == today_tw else {}

        rows.append({
            "reservoir_id": rid,
            "reservoir_name": best_name(rid, d.get("reservoir_name"), current_daily_ops_map, basic_info_map, manual_overrides),
            "date": date_str,
            "observation_time": r.get("observation_time") or d.get("observation_time") or c.get("observation_time") or "",
            "water_level_m": r.get("water_level_m"),
            "percentage_storage_pct": r.get("percentage_storage_pct"),
            "effective_storage_capacity_10k_m3": (
                r.get("effective_storage_capacity_10k_m3")
                if r.get("effective_storage_capacity_10k_m3") is not None
                else (
                    d.get("effective_storage_capacity_10k_m3")
                    if d.get("effective_storage_capacity_10k_m3") is not None
                    else c.get("effective_storage_capacity_10k_m3")
                )
            ),
            "rainfall_in_catchment_mm": (
                r.get("rainfall_in_catchment_mm")
                if r.get("rainfall_in_catchment_mm") is not None
                else (
                    d.get("rainfall_in_catchment_mm")
                    if d.get("rainfall_in_catchment_mm") is not None
                    else c.get("rainfall_in_catchment_mm")
                )
            ),
            "inflow_total_10k_m3": (
                d.get("inflow_total_10k_m3")
                if d.get("inflow_total_10k_m3") is not None
                else c.get("inflow_total_10k_m3")
            ),
            "outflow_total_10k_m3": (
                r.get("outflow_total_10k_m3")
                if r.get("outflow_total_10k_m3") is not None
                else (
                    d.get("outflow_total_10k_m3")
                    if d.get("outflow_total_10k_m3") is not None
                    else c.get("outflow_total_10k_m3")
                )
            ),
            "water_draw_10k_m3": (
                r.get("water_draw_10k_m3")
                if r.get("water_draw_10k_m3") is not None
                else (
                    d.get("water_draw_10k_m3")
                    if d.get("water_draw_10k_m3") is not None
                    else c.get("water_draw_10k_m3")
                )
            ),
            "predetermined_crossflow_10k_m3": (
                r.get("predetermined_crossflow_10k_m3")
                if r.get("predetermined_crossflow_10k_m3") is not None
                else (
                    d.get("predetermined_crossflow_10k_m3")
                    if d.get("predetermined_crossflow_10k_m3") is not None
                    else c.get("predetermined_crossflow_10k_m3")
                )
            ),
            "desilting_tunnel_outflow_10k_m3": (
                r.get("desilting_tunnel_outflow_10k_m3")
                if r.get("desilting_tunnel_outflow_10k_m3") is not None
                else d.get("desilting_tunnel_outflow_10k_m3")
            ),
            "drainage_tunnel_outflow_10k_m3": (
                r.get("drainage_tunnel_outflow_10k_m3")
                if r.get("drainage_tunnel_outflow_10k_m3") is not None
                else d.get("drainage_tunnel_outflow_10k_m3")
            ),
            "power_outlet_outflow_10k_m3": (
                r.get("power_outlet_outflow_10k_m3")
                if r.get("power_outlet_outflow_10k_m3") is not None
                else (
                    d.get("power_outlet_outflow_10k_m3")
                    if d.get("power_outlet_outflow_10k_m3") is not None
                    else c.get("power_outlet_outflow_10k_m3")
                )
            ),
            "spillway_outflow_10k_m3": (
                r.get("spillway_outflow_10k_m3")
                if r.get("spillway_outflow_10k_m3") is not None
                else (
                    d.get("spillway_outflow_10k_m3")
                    if d.get("spillway_outflow_10k_m3") is not None
                    else c.get("spillway_outflow_10k_m3")
                )
            ),
            "others_outflow_10k_m3": (
                r.get("others_outflow_10k_m3")
                if r.get("others_outflow_10k_m3") is not None
                else d.get("others_outflow_10k_m3")
            ),
            "status_type": r.get("status_type") or d.get("status_type"),
        })
    return rows


def write_timeseries_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([c for c, _ in TS_COLUMNS])
        for row in rows:
            writer.writerow([row.get(key, "") if row.get(key) is not None else "" for _, key in TS_COLUMNS])


def write_intraday_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([c for c, _ in INTRADAY_COLUMNS])
        for row in rows:
            writer.writerow([row.get(key, "") if row.get(key) is not None else "" for _, key in INTRADAY_COLUMNS])


def upsert_metadata(
    path: Path,
    basic_info_map: dict[str, dict],
    current_daily_ops_map: dict[str, dict],
    manual_overrides: dict[str, dict],
) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing: "OrderedDict[str, dict]" = OrderedDict()
    if path.exists():
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                rid = row.get("reservoir_id")
                if rid:
                    existing[rid] = row

    ids = set(existing) | set(basic_info_map) | set(current_daily_ops_map) | set(manual_overrides)
    for rid in sorted(ids):
        b = basic_info_map.get(rid, {})
        c = current_daily_ops_map.get(rid, {})
        m = manual_overrides.get(rid, {})
        existing[rid] = {
            "reservoir_id": rid,
            "reservoir_name": (
                c.get("reservoir_name")
                or b.get("reservoir_name")
                or m.get("reservoir_name")
                or existing.get(rid, {}).get("reservoir_name")
                or ""
            ),
            "reservoir_name_en": "",
            "country": "Taiwan",
            "admin_unit": b.get("admin_unit") or m.get("admin_unit") or existing.get(rid, {}).get("admin_unit") or "",
            "river": b.get("river") or m.get("river") or existing.get(rid, {}).get("river") or "",
            "basin": b.get("basin") or m.get("basin") or existing.get(rid, {}).get("basin") or "",
            "lat": "",
            "lon": "",
            "source_system": b.get("source_system") or m.get("source_system") or "WRA Open Data",
            "source_agency": SOURCE_AGENCY,
            "source_url": SOURCE_URL,
            "last_updated": now,
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=META_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing.values())
    return len(existing)


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    output_dir = Path(os.environ.get("OUTPUT_DIR", str(script_dir / "taiwan_wra_outputs"))).resolve()
    dirs = ensure_dirs(output_dir)
    manual_overrides = load_manual_overrides(script_dir / "manual_name_overrides.csv")
    manual_backfill = bool(os.environ.get("TAIWAN_START_DATE") or os.environ.get("TAIWAN_END_DATE"))
    skip_existing_env = os.environ.get("SKIP_EXISTING_DAILY")
    skip_existing = (skip_existing_env != "0") if skip_existing_env is not None else (not manual_backfill)
    save_raw = os.environ.get("SAVE_RAW_JSON", "1") != "0"
    dates = target_dates()
    today_tw = datetime.now(TAIWAN_TZ).date().isoformat()

    session = requests.Session()
    session.headers.update(HEADERS)

    summary: dict[str, Any] = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(output_dir),
        "dates": dates,
        "files_written": [],
        "errors": [],
        "records_per_date": {},
    }

    try:
        print(f"[INFO] OUTPUT_DIR = {output_dir}")
        basic_info_map: dict[str, dict] = {}
        try:
            basic_rows = get_json(session, BASIC_INFO_URL)
            basic_info_map = normalize_basic_info(basic_rows if isinstance(basic_rows, list) else [])
            if save_raw:
                save_json(dirs["raw"] / "basic_info.json", basic_rows)
                summary["files_written"].append(str(dirs["raw"] / "basic_info.json"))
        except Exception as e:
            print(f"[WARN] basic info dataset unavailable: {e}", file=sys.stderr)
            summary["errors"].append({"basic_info_warning": str(e)})

        current_daily_ops_map: dict[str, dict] = {}
        try:
            current_daily_rows = get_json(session, CURRENT_DAILY_OPS_URL)
            current_daily_ops_map = normalize_current_daily_ops(
                current_daily_rows if isinstance(current_daily_rows, list) else []
            )
            if save_raw:
                current_daily_path = dirs["raw"] / f"current_daily_ops_{today_tw}.json"
                save_json(current_daily_path, current_daily_rows)
                summary["files_written"].append(str(current_daily_path))
        except Exception as e:
            print(f"[WARN] current daily ops dataset unavailable: {e}", file=sys.stderr)
            summary["errors"].append({"current_daily_ops_warning": str(e)})

        current_water_level_map: dict[str, dict] = {}
        try:
            water_level_rows = get_json(session, CURRENT_WATER_LEVEL_URL)
            current_water_level_map = normalize_current_water_level(
                water_level_rows if isinstance(water_level_rows, list) else []
            )
            intraday_rows = normalize_current_water_level_intraday(
                water_level_rows if isinstance(water_level_rows, list) else [],
                basic_info_map,
                current_daily_ops_map,
                manual_overrides,
            )
            if save_raw:
                water_level_path = dirs["raw"] / f"current_water_level_{today_tw}.json"
                save_json(water_level_path, water_level_rows)
                summary["files_written"].append(str(water_level_path))
            intraday_path = dirs["intraday"] / f"taiwan_intraday_{today_tw}.csv"
            write_intraday_csv(intraday_path, intraday_rows)
            summary["files_written"].append(str(intraday_path))
        except Exception as e:
            print(f"[WARN] current water level dataset unavailable: {e}", file=sys.stderr)
            summary["errors"].append({"current_water_level_warning": str(e)})

        daily_success = 0
        daily_skipped = 0
        for date_str in dates:
            daily_path = dirs["daily"] / f"taiwan_timeseries_{date_str}.csv"
            # Keep today's file fresh because names/current water level come from
            # current datasets and can improve over earlier runs. Historical files
            # can stay immutable once written.
            if skip_existing and daily_path.exists() and date_str != today_tw:
                print(f"[SKIP] {daily_path.name}")
                daily_skipped += 1
                continue

            print(f"[FETCH] {date_str}")
            daily_rows = get_json(session, HIST_DAILY_URL.format(date=date_str))
            if save_raw:
                raw_path = dirs["raw_daily"] / f"{date_str}.json"
                save_json(raw_path, daily_rows)
                summary["files_written"].append(str(raw_path))

            daily_map = normalize_daily(daily_rows if isinstance(daily_rows, list) else [], date_str)
            rows = build_rows(
                date_str,
                basic_info_map,
                current_daily_ops_map,
                daily_map,
                current_water_level_map,
                manual_overrides,
                today_tw,
            )
            write_timeseries_csv(daily_path, rows)
            print(f"[OK] {daily_path.name} ({len(rows)} rows)")
            summary["records_per_date"][date_str] = len(rows)
            summary["files_written"].append(str(daily_path))
            daily_success += 1
            time.sleep(0.4)

        if daily_success == 0 and daily_skipped != len(dates):
            raise RuntimeError("No Taiwan daily datasets were fetched successfully.")

        count = upsert_metadata(
            dirs["metadata"] / "taiwan_wra_reservoirs.csv",
            basic_info_map,
            current_daily_ops_map,
            manual_overrides,
        )
        print(f"[METADATA] {count} reservoirs")
        summary["files_written"].append(str(dirs["metadata"] / "taiwan_wra_reservoirs.csv"))
        summary["metadata_count"] = count
        unresolved = sorted({
            r["reservoir_id"]
            for date_str in summary["records_per_date"]
            for r in []
        })
        unresolved = []
        for daily_file in sorted(dirs["daily"].glob("taiwan_timeseries_*.csv")):
            with open(daily_file, encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    if not clean_value(row.get("reservoir_name")):
                        unresolved.append(row.get("reservoir_id"))
        unresolved = sorted(set(x for x in unresolved if x))
        summary["unresolved_name_ids"] = unresolved
        if unresolved:
            print(f"[WARN] unresolved reservoir names: {', '.join(unresolved)}", file=sys.stderr)
        summary["status"] = "ok"
        return_code = 0
    except Exception as e:
        summary["status"] = "error"
        summary["errors"].append({"error": str(e), "traceback": traceback.format_exc()})
        print(traceback.format_exc(), file=sys.stderr)
        return_code = 1
    finally:
        summary["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        ts = datetime.now(TAIWAN_TZ).strftime("%Y%m%d_%H%M%S")
        log_path = dirs["logs"] / f"{ts}_summary.json"
        write_summary(log_path, summary)
        print(f"[SUMMARY] {log_path}")

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())

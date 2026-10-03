"""Taiwan WRA reservoir scraper.

Sources (WRA open data, opendata.wra.gov.tw):
- Daily operations: one day per publication, the previous day's values
- Current water level: the latest observations (intraday table)
- Annual reservoir basic information (metadata)

The keyless history API (fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily) was
retired in June 2026: every v1 path has answered "HTTP Error 503. The service
is unavailable." since 2026-06-13. Its successor, FHY General API v2, needs a
key that WRA issues to government bodies; its documentation sends other users
to the open-data platform. So no source can supply a past day: each daily
table is the official daily-operations snapshot, filed under the date the
source reports, and a day the schedule misses is lost (logged as
missed_dates).
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
import traceback
from collections import Counter, OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


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
REQUEST_ATTEMPTS = 3
REQUEST_BACKOFFS = (2, 8, 20)

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
    "dam_type",
    "dam_height (m)",
    "dam_length (m)",
    "catchment_area (hectare)",
    "surface_area_frl (hectare)",
    "capacity_design_total (10^4 m^3)",
    "capacity_design_effective (10^4 m^3)",
    "capacity_current_total (10^4 m^3)",
    "capacity_current_effective (10^4 m^3)",
    "main_use",
    "operator",
    "last_capacity_survey_year_roc",
    "coord_source",
    "source_system",
    "source_agency",
    "source_url",
    "last_updated",
]


def load_reservoir_coords(path: Path) -> dict[str, dict]:
    """Load static lat/lon lookup derived from WRA GIS shapefiles.

    Keyed by reservoir_id. Values include lat, lon, coord_source.
    """
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rid = clean_value(row.get("reservoir_id"))
            if not rid:
                continue
            out[str(rid)] = {
                "lat": clean_value(row.get("lat")) or "",
                "lon": clean_value(row.get("lon")) or "",
                "coord_source": clean_value(row.get("coord_source")) or "",
            }
    return out


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
    last_exc: Exception | None = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < REQUEST_ATTEMPTS:
                time.sleep(REQUEST_BACKOFFS[min(attempt - 1, len(REQUEST_BACKOFFS) - 1)])
        except Exception:
            raise
    assert last_exc is not None
    raise last_exc


def emit_workflow_warning(message: str, title: str = "Taiwan WRA source availability") -> None:
    print(f"::warning title={title}::{message}")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "").strip()
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(f"### {title}\n\n{message}\n")


def missed_dates_before(daily_dir: Path, snapshot_date: str) -> list[str]:
    """Days between the newest archived daily table older than `snapshot_date`
    and `snapshot_date` itself. The open-data snapshot holds one day, so these
    can no longer be fetched from any source the scraper may use."""
    earlier = []
    for path in daily_dir.glob("taiwan_timeseries_*.csv"):
        try:
            day = datetime.strptime(path.stem.rsplit("_", 1)[1], "%Y-%m-%d").date()
        except ValueError:
            continue
        if day.isoformat() < snapshot_date:
            earlier.append(day)
    if not earlier:
        return []
    previous = max(earlier)
    end = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
    return [(previous + timedelta(days=n)).isoformat() for n in range(1, (end - previous).days)]


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


def _numeric(v: Any) -> Any:
    """Parse number-like strings (with commas/spaces) while leaving empties as ''."""
    v = clean_value(v)
    if v is None:
        return ""
    s = str(v).replace(",", "").strip()
    if not s:
        return ""
    try:
        f = float(s)
        return int(f) if f.is_integer() else f
    except Exception:
        return ""


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
            "dam_type": clean_value(row.get("型式")) or "",
            "dam_height_m": _numeric(row.get("壩堰高")),
            "dam_length_m": _numeric(row.get("壩堰長")),
            "catchment_area_ha": _numeric(row.get("集水面積")),
            "surface_area_ha": _numeric(row.get("滿水位面積")),
            "capacity_design_total": _numeric(row.get("設計總容量")),
            "capacity_design_effective": _numeric(row.get("設計有效容量")),
            "capacity_current_total": _numeric(row.get("目前總容量")),
            "capacity_current_effective": _numeric(row.get("目前有效容量")),
            "main_use": clean_value(row.get("功能")) or "",
            "operator": clean_value(row.get("機關名稱")) or "",
            "last_capacity_survey_year_roc": _numeric(row.get("最近完成庫容測量時間")),
            "source_system": "opendata.wra.gov.tw Basic Information",
        }
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


def select_current_daily_snapshot(
    current_daily_ops_map: dict[str, dict],
) -> tuple[str | None, dict[str, dict], dict[str, int]]:
    """Select the dominant source-reported date without relabelling observations."""
    date_counts = Counter(
        str(row.get("observation_time") or "")[:10]
        for row in current_daily_ops_map.values()
        if row.get("observation_time")
    )
    if not date_counts:
        return None, {}, {}
    snapshot_date, count = max(date_counts.items(), key=lambda pair: (pair[1], pair[0]))
    if count < max(1, len(current_daily_ops_map) // 2):
        return None, {}, dict(sorted(date_counts.items()))
    snapshot_map = {
        rid: row
        for rid, row in current_daily_ops_map.items()
        if str(row.get("observation_time") or "").startswith(snapshot_date)
    }
    return snapshot_date, snapshot_map, dict(sorted(date_counts.items()))


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
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow([c for c, _ in TS_COLUMNS])
        for row in rows:
            writer.writerow([row.get(key, "") if row.get(key) is not None else "" for _, key in TS_COLUMNS])


def write_intraday_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow([c for c, _ in INTRADAY_COLUMNS])
        for row in rows:
            writer.writerow([row.get(key, "") if row.get(key) is not None else "" for _, key in INTRADAY_COLUMNS])


def build_current_snapshot_rows(
    snapshot_date: str,
    snapshot_map: dict[str, dict],
    basic_info_map: dict[str, dict],
    manual_overrides: dict[str, dict],
) -> list[dict]:
    """Build a daily table using only reservoirs observed on the reported date."""
    ids = set(snapshot_map)
    return build_rows(
        snapshot_date,
        {rid: basic_info_map.get(rid, {}) for rid in ids},
        snapshot_map,
        snapshot_map,
        {},
        {rid: manual_overrides.get(rid, {}) for rid in ids},
        "",
    )


def backfill_archived_current_daily(
    dirs: dict[str, Path],
    basic_info_map: dict[str, dict],
    manual_overrides: dict[str, dict],
) -> list[dict[str, Any]]:
    """Recover source-dated daily snapshots from already archived official JSON."""
    recovered: list[dict[str, Any]] = []
    for raw_path in sorted(dirs["raw"].glob("current_daily_ops_*.json")):
        try:
            payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(
                f"[WARN] cannot read archived current daily data {raw_path}: {exc}",
                file=sys.stderr,
            )
            continue
        if not isinstance(payload, list):
            continue
        snapshot_date, snapshot_map, _ = select_current_daily_snapshot(
            normalize_current_daily_ops(payload)
        )
        if not snapshot_date or not snapshot_map:
            continue
        daily_path = dirs["daily"] / f"taiwan_timeseries_{snapshot_date}.csv"
        if daily_path.exists():
            continue
        rows = build_current_snapshot_rows(
            snapshot_date, snapshot_map, basic_info_map, manual_overrides
        )
        write_timeseries_csv(daily_path, rows)
        print(
            f"[RECOVER] {daily_path.name} from {raw_path.name} ({len(rows)} rows)"
        )
        recovered.append(
            {
                "date": snapshot_date,
                "rows": len(rows),
                "source_raw": str(raw_path),
                "output": str(daily_path),
            }
        )
    return recovered


def upsert_metadata(
    path: Path,
    basic_info_map: dict[str, dict],
    current_daily_ops_map: dict[str, dict],
    manual_overrides: dict[str, dict],
    coords_map: dict[str, dict],
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

    def _pick(key_b, *fallbacks):
        """Prefer basic_info value; fall back to existing CSV value."""
        val = b.get(key_b)
        if val not in (None, ""):
            return val
        for src in fallbacks:
            v = src.get(key_b) if isinstance(src, dict) else None
            if v not in (None, ""):
                return v
        return ""

    for rid in sorted(ids):
        b = basic_info_map.get(rid, {})
        c = current_daily_ops_map.get(rid, {})
        m = manual_overrides.get(rid, {})
        coord = coords_map.get(rid, {})
        prev = existing.get(rid, {})
        existing[rid] = {
            "reservoir_id": rid,
            "reservoir_name": (
                c.get("reservoir_name")
                or b.get("reservoir_name")
                or m.get("reservoir_name")
                or prev.get("reservoir_name")
                or ""
            ),
            "reservoir_name_en": prev.get("reservoir_name_en") or "",
            "country": "Taiwan",
            "admin_unit": b.get("admin_unit") or m.get("admin_unit") or prev.get("admin_unit") or "",
            "river": b.get("river") or m.get("river") or prev.get("river") or "",
            "basin": b.get("basin") or m.get("basin") or prev.get("basin") or "",
            "lat": coord.get("lat") or prev.get("lat") or "",
            "lon": coord.get("lon") or prev.get("lon") or "",
            "dam_type": b.get("dam_type") or prev.get("dam_type") or "",
            "dam_height (m)": b.get("dam_height_m", "") if b.get("dam_height_m", "") != "" else prev.get("dam_height (m)", ""),
            "dam_length (m)": b.get("dam_length_m", "") if b.get("dam_length_m", "") != "" else prev.get("dam_length (m)", ""),
            "catchment_area (hectare)": b.get("catchment_area_ha", "") if b.get("catchment_area_ha", "") != "" else prev.get("catchment_area (hectare)", ""),
            "surface_area_frl (hectare)": b.get("surface_area_ha", "") if b.get("surface_area_ha", "") != "" else prev.get("surface_area_frl (hectare)", ""),
            "capacity_design_total (10^4 m^3)": b.get("capacity_design_total", "") if b.get("capacity_design_total", "") != "" else prev.get("capacity_design_total (10^4 m^3)", ""),
            "capacity_design_effective (10^4 m^3)": b.get("capacity_design_effective", "") if b.get("capacity_design_effective", "") != "" else prev.get("capacity_design_effective (10^4 m^3)", ""),
            "capacity_current_total (10^4 m^3)": b.get("capacity_current_total", "") if b.get("capacity_current_total", "") != "" else prev.get("capacity_current_total (10^4 m^3)", ""),
            "capacity_current_effective (10^4 m^3)": b.get("capacity_current_effective", "") if b.get("capacity_current_effective", "") != "" else prev.get("capacity_current_effective (10^4 m^3)", ""),
            "main_use": b.get("main_use") or prev.get("main_use") or "",
            "operator": b.get("operator") or prev.get("operator") or "",
            "last_capacity_survey_year_roc": b.get("last_capacity_survey_year_roc", "") if b.get("last_capacity_survey_year_roc", "") != "" else prev.get("last_capacity_survey_year_roc", ""),
            "coord_source": coord.get("coord_source") or prev.get("coord_source") or "",
            "source_system": b.get("source_system") or m.get("source_system") or "WRA Open Data",
            "source_agency": SOURCE_AGENCY,
            "source_url": SOURCE_URL,
            "last_updated": now,
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=META_COLUMNS,
            extrasaction="ignore",
            lineterminator="\n",
        )
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
    coords_map = load_reservoir_coords(script_dir / "reservoir_coords.csv")
    save_raw = os.environ.get("SAVE_RAW_JSON", "1") != "0"
    today_tw = datetime.now(TAIWAN_TZ).date().isoformat()

    session = requests.Session()
    session.headers.update(HEADERS)

    summary: dict[str, Any] = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(output_dir),
        "files_written": [],
        "errors": [],
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

        current_daily_rows: list[dict] = []
        current_daily_ops_map: dict[str, dict] = {}
        current_daily_snapshot_date: str | None = None
        current_daily_snapshot_map: dict[str, dict] = {}
        snapshot_was_archived = False
        try:
            current_daily_rows = get_json(session, CURRENT_DAILY_OPS_URL)
            current_daily_ops_map = normalize_current_daily_ops(
                current_daily_rows if isinstance(current_daily_rows, list) else []
            )
            (
                current_daily_snapshot_date,
                current_daily_snapshot_map,
                current_daily_date_counts,
            ) = select_current_daily_snapshot(current_daily_ops_map)
            if current_daily_snapshot_date:
                snapshot_was_archived = (
                    dirs["daily"] / f"taiwan_timeseries_{current_daily_snapshot_date}.csv"
                ).exists()
                summary["current_daily_snapshot"] = {
                    "date": current_daily_snapshot_date,
                    "rows": len(current_daily_snapshot_map),
                    "date_counts": current_daily_date_counts,
                }
            if save_raw:
                current_daily_path = dirs["raw"] / f"current_daily_ops_{today_tw}.json"
                save_json(current_daily_path, current_daily_rows)
                summary["files_written"].append(str(current_daily_path))
        except Exception as e:
            print(f"[WARN] current daily ops dataset unavailable: {e}", file=sys.stderr)
            summary["errors"].append({"current_daily_ops_warning": str(e)})

        try:
            water_level_rows = get_json(session, CURRENT_WATER_LEVEL_URL)
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

        recovered = backfill_archived_current_daily(
            dirs, basic_info_map, manual_overrides
        )
        if recovered:
            summary["archived_current_daily_backfill"] = recovered
            summary["files_written"].extend(item["output"] for item in recovered)

        if current_daily_snapshot_date:
            daily_path = dirs["daily"] / f"taiwan_timeseries_{current_daily_snapshot_date}.csv"
            if not daily_path.exists():
                # Only without SAVE_RAW_JSON; otherwise the backfill above has
                # already filed the snapshot from its archived raw copy.
                rows = build_current_snapshot_rows(
                    current_daily_snapshot_date,
                    current_daily_snapshot_map,
                    basic_info_map,
                    manual_overrides,
                )
                write_timeseries_csv(daily_path, rows)
                summary["files_written"].append(str(daily_path))
                print(f"[OK] {daily_path.name} ({len(rows)} rows)")
            summary["daily_snapshot_status"] = (
                "already_archived" if snapshot_was_archived else "archived"
            )
            # Only the run that files a date reports the gap before it, so a
            # gap is reported once rather than by every later run that day.
            missed = (
                [] if snapshot_was_archived
                else missed_dates_before(dirs["daily"], current_daily_snapshot_date)
            )
            if missed:
                summary["missed_dates"] = missed
                emit_workflow_warning(
                    f"No daily table was archived for {len(missed)} day(s) before "
                    f"{current_daily_snapshot_date} ({missed[0]} to {missed[-1]}). "
                    "The open-data snapshot holds one day, so they cannot be fetched now.",
                    title="Taiwan WRA missed days",
                )
        else:
            summary["daily_snapshot_status"] = "unavailable"
            emit_workflow_warning(
                "The daily-operations dataset gave no usable snapshot this run. "
                "Each day stays in the dataset until it rolls over, so the next "
                "scheduled run can still file it."
            )

        count = upsert_metadata(
            dirs["metadata"] / "taiwan_wra_reservoirs.csv",
            basic_info_map,
            current_daily_ops_map,
            manual_overrides,
            coords_map,
        )
        print(f"[METADATA] {count} reservoirs")
        summary["files_written"].append(str(dirs["metadata"] / "taiwan_wra_reservoirs.csv"))
        summary["metadata_count"] = count
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
        summary["status"] = (
            "ok" if current_daily_snapshot_date else "source_unavailable"
        )
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

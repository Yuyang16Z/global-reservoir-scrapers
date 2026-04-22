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


STATIC_URL = "https://data.wra.gov.tw/Service/OpenData.aspx?format=json&id=E2_7_00001"
DAILY_URL = "https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date={date}"
REALTIME_URL = "https://fhy.wra.gov.tw/WraApi/v1/Reservoir/RealTime"
SOURCE_URL = "https://fhy.wra.gov.tw/fhyv2/monitor/reservoir"
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
    for key in ("ReservoirIdentifier", "reservoiridentifier", "StationNo", "stationno"):
        v = clean_value(row.get(key))
        if v is not None:
            return str(v)
    return None


def get_name(row: dict) -> str | None:
    for key in ("ReservoirName", "reservoirname", "水庫名稱", "Reservoir", "Name"):
        v = clean_value(row.get(key))
        if v is not None:
            return str(v)
    return None


def normalize_static(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        out[rid] = {
            "reservoir_id": rid,
            "reservoir_name": get_name(row),
            "source_system": "data.wra.gov.tw OpenData",
        }
    return out


def normalize_realtime(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        rid = get_id(row)
        if not rid:
            continue
        out[rid] = {
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
    static_map: dict[str, dict],
    daily_map: dict[str, dict],
    realtime_map: dict[str, dict],
    today_tw: str,
) -> list[dict]:
    ids = set(static_map) | set(daily_map)
    if date_str == today_tw:
        ids |= set(realtime_map)

    rows: list[dict] = []
    for rid in sorted(ids):
        s = static_map.get(rid, {})
        d = daily_map.get(rid, {})
        r = realtime_map.get(rid, {}) if date_str == today_tw else {}

        rows.append({
            "reservoir_id": rid,
            "reservoir_name": d.get("reservoir_name") or s.get("reservoir_name") or "",
            "date": date_str,
            "observation_time": r.get("observation_time") or d.get("observation_time") or "",
            "water_level_m": r.get("water_level_m"),
            "percentage_storage_pct": r.get("percentage_storage_pct"),
            "effective_storage_capacity_10k_m3": (
                r.get("effective_storage_capacity_10k_m3")
                if r.get("effective_storage_capacity_10k_m3") is not None
                else d.get("effective_storage_capacity_10k_m3")
            ),
            "rainfall_in_catchment_mm": (
                r.get("rainfall_in_catchment_mm")
                if r.get("rainfall_in_catchment_mm") is not None
                else d.get("rainfall_in_catchment_mm")
            ),
            "inflow_total_10k_m3": d.get("inflow_total_10k_m3"),
            "outflow_total_10k_m3": (
                r.get("outflow_total_10k_m3")
                if r.get("outflow_total_10k_m3") is not None
                else d.get("outflow_total_10k_m3")
            ),
            "water_draw_10k_m3": (
                r.get("water_draw_10k_m3")
                if r.get("water_draw_10k_m3") is not None
                else d.get("water_draw_10k_m3")
            ),
            "predetermined_crossflow_10k_m3": (
                r.get("predetermined_crossflow_10k_m3")
                if r.get("predetermined_crossflow_10k_m3") is not None
                else d.get("predetermined_crossflow_10k_m3")
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
                else d.get("power_outlet_outflow_10k_m3")
            ),
            "spillway_outflow_10k_m3": (
                r.get("spillway_outflow_10k_m3")
                if r.get("spillway_outflow_10k_m3") is not None
                else d.get("spillway_outflow_10k_m3")
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


def upsert_metadata(path: Path, static_map: dict[str, dict], daily_map: dict[str, dict]) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing: "OrderedDict[str, dict]" = OrderedDict()
    if path.exists():
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                rid = row.get("reservoir_id")
                if rid:
                    existing[rid] = row

    ids = set(existing) | set(static_map) | set(daily_map)
    for rid in sorted(ids):
        s = static_map.get(rid, {})
        d = daily_map.get(rid, {})
        existing[rid] = {
            "reservoir_id": rid,
            "reservoir_name": d.get("reservoir_name") or s.get("reservoir_name") or existing.get(rid, {}).get("reservoir_name") or "",
            "reservoir_name_en": "",
            "country": "Taiwan",
            "admin_unit": "",
            "river": "",
            "basin": "",
            "lat": "",
            "lon": "",
            "source_system": s.get("source_system") or "WRA API",
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
    skip_existing = os.environ.get("SKIP_EXISTING_DAILY", "1") != "0"
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
        static_map: dict[str, dict] = {}
        try:
            static_rows = get_json(session, STATIC_URL)
            static_map = normalize_static(static_rows if isinstance(static_rows, list) else [])
            if save_raw:
                save_json(dirs["raw"] / "static_reservoirs.json", static_rows)
                summary["files_written"].append(str(dirs["raw"] / "static_reservoirs.json"))
        except Exception as e:
            print(f"[WARN] static dataset unavailable: {e}", file=sys.stderr)
            summary["errors"].append({"static_warning": str(e)})

        realtime_map: dict[str, dict] = {}
        try:
            realtime_rows = get_json(session, REALTIME_URL)
            realtime_map = normalize_realtime(realtime_rows if isinstance(realtime_rows, list) else [])
            if save_raw:
                rt_path = dirs["raw"] / f"realtime_{today_tw}.json"
                save_json(rt_path, realtime_rows)
                summary["files_written"].append(str(rt_path))
        except Exception as e:
            print(f"[WARN] realtime dataset unavailable: {e}", file=sys.stderr)
            summary["errors"].append({"realtime_warning": str(e)})

        metadata_daily_union: dict[str, dict] = {}
        daily_success = 0
        for date_str in dates:
            daily_path = dirs["daily"] / f"taiwan_timeseries_{date_str}.csv"
            if skip_existing and daily_path.exists():
                print(f"[SKIP] {daily_path.name}")
                continue

            print(f"[FETCH] {date_str}")
            daily_rows = get_json(session, DAILY_URL.format(date=date_str))
            if save_raw:
                raw_path = dirs["raw_daily"] / f"{date_str}.json"
                save_json(raw_path, daily_rows)
                summary["files_written"].append(str(raw_path))

            daily_map = normalize_daily(daily_rows if isinstance(daily_rows, list) else [], date_str)
            metadata_daily_union.update(daily_map)
            rows = build_rows(date_str, static_map, daily_map, realtime_map, today_tw)
            write_timeseries_csv(daily_path, rows)
            print(f"[OK] {daily_path.name} ({len(rows)} rows)")
            summary["records_per_date"][date_str] = len(rows)
            summary["files_written"].append(str(daily_path))
            daily_success += 1
            time.sleep(0.4)

        if daily_success == 0 and not metadata_daily_union:
            raise RuntimeError("No Taiwan daily datasets were fetched successfully.")

        count = upsert_metadata(dirs["metadata"] / "taiwan_wra_reservoirs.csv", static_map, metadata_daily_union)
        print(f"[METADATA] {count} reservoirs")
        summary["files_written"].append(str(dirs["metadata"] / "taiwan_wra_reservoirs.csv"))
        summary["metadata_count"] = count
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

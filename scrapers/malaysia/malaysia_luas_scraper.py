"""
Malaysia — LUAS (Lembaga Urus Air Selangor) IWRIMS reservoir scraper.

Source: https://iwrims.luas.gov.my/
Endpoint: /getMapData_JSON.cfm?data=damstation  (public, no login)
Coverage: Selangor state only (8 reservoirs + 1 barrage)
Update cadence: source refreshes water level / storage / release daily (roughly every 1-8h).
This script polls once per run and saves a daily snapshot.

Output directory:
- Local run (no env): ./malaysia_luas_outputs/
- CI / overridden:     $OUTPUT_DIR (absolute or relative to CWD)

Output layout (per schema.md):
  <OUTPUT_DIR>/
    metadata/malaysia_luas_reservoirs.csv        # one row per reservoir (static)
    timeseries/daily/malaysia_luas_timeseries_YYYY-MM-DD.csv
    raw/damstation_YYYYMMDD_HHMMSS.json          # raw JSON per poll
    raw/barrage_YYYYMMDD_HHMMSS.json
    run_logs/<stamp>_summary.json
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


BASE_DIR = Path(__file__).resolve().parent

_env_out = os.environ.get("OUTPUT_DIR", "").strip()
if _env_out:
    OUTPUT_DIR = Path(_env_out).expanduser().resolve()
else:
    OUTPUT_DIR = BASE_DIR / "malaysia_luas_outputs"

METADATA_DIR = OUTPUT_DIR / "metadata"
DAILY_DIR = OUTPUT_DIR / "timeseries" / "daily"
RAW_DIR = OUTPUT_DIR / "raw"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

HOST = "https://iwrims.luas.gov.my"
REFERER = f"{HOST}/main.cfm"
DAM_URL = f"{HOST}/getMapData_JSON.cfm?data=damstation"
BARRAGE_URL = f"{HOST}/getMapData_JSON.cfm?data=barrage"
TIMEOUT = 30

SOURCE_AGENCY = "LUAS"
SOURCE_BASE_URL = "https://iwrims.luas.gov.my/"

STATE_NAMES = {"SEL": "Selangor"}


METADATA_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "reservoir_name_en",
    "country",
    "admin_unit",
    "river",
    "basin",
    "lat",
    "lon",
    "capacity_total (mcm, 10^6 m^3)",
    "dead_storage (mcm, 10^6 m^3)",
    "frl (normal pool level, m)",
    "alert_level (m)",
    "max_effective (m)",
    "dam_height (m)",
    "catchment (sq.km)",
    "station_type (0=dam, 5=barrage)",
    "source_agency",
    "source_url",
    "last_updated",
]

SNAPSHOT_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "DATE_UPDATE (source observation timestamp, local time)",
    "WATER_LEVEL (water level, m)",
    "STORAGE (current storage, mcm / 10^6 m^3)",
    "STORAGE_PERCENT (percent full, %)",
    "RELEASE_MLD (release / outflow, mld = 10^6 liters per day)",
    "SPILL (spillway release, mld)",
    "RAIN (rainfall, mm)",
    "CUMM_RAIN (cumulative rainfall, mm)",
    "RESIDUAL (residual flow, mld)",
    "RESERVOIR (reservoir flow, mld)",
    "CONTROL_VOLUME (control volume, mcm)",
]


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_value(raw: Any) -> Any:
    if raw is None:
        return ""
    if isinstance(raw, str) and raw.strip() == "":
        return ""
    return raw


def fetch_json(url: str, session: requests.Session) -> list[dict]:
    resp = session.get(
        url,
        headers={
            "Accept": "application/json, text/javascript, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": REFERER,
        },
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    text = resp.text.strip()
    if not text:
        return []
    return json.loads(text)


def extract_obs_date(record: dict) -> str:
    du = record.get("DATE_UPDATE", "")
    if isinstance(du, str) and du:
        try:
            return datetime.strptime(du.split(" ")[0], "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    dt = record.get("DATE_TAKEN", "")
    if isinstance(dt, str) and dt:
        try:
            return datetime.strptime(dt.strip(), "%B, %d %Y %H:%M:%S").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d")


def build_reservoir_id(record: dict, stn_type: int) -> str:
    dam_id = record.get("DAM_ID")
    if isinstance(dam_id, int) and dam_id > 0:
        return f"MY_LUAS_{dam_id}"
    stn_id = record.get("STATIONID")
    if isinstance(stn_id, int) and stn_id > 0:
        return f"MY_LUAS_STN{stn_id}"
    name = str(record.get("STATION_NAME", "")).strip() or "unknown"
    return f"MY_LUAS_{name.replace(' ', '_')}"


def build_metadata_row(record: dict, stn_type: int, fetched_at: str) -> dict:
    state_code = record.get("STATE", "") or ""
    basin = record.get("RIVER_BASIN", "") or ""
    return {
        "reservoir_id": build_reservoir_id(record, stn_type),
        "reservoir_name": record.get("STATION_FULLNAME", "") or record.get("STATION_NAME", ""),
        "reservoir_name_en": record.get("STATION_FULLNAME", "") or record.get("STATION_NAME", ""),
        "country": "Malaysia",
        "admin_unit": STATE_NAMES.get(state_code, state_code),
        "river": basin,
        "basin": basin,
        "lat": safe_value(record.get("LATITUDE")),
        "lon": safe_value(record.get("LONGITUDE")),
        "capacity_total (mcm, 10^6 m^3)": safe_value(record.get("MAX_STORAGE")),
        "dead_storage (mcm, 10^6 m^3)": safe_value(record.get("DEAD_STORAGE")),
        "frl (normal pool level, m)": safe_value(record.get("NORMAL")),
        "alert_level (m)": safe_value(record.get("ALERT")),
        "max_effective (m)": safe_value(record.get("MAX_EFFECTIVE")),
        "dam_height (m)": safe_value(record.get("HEIGHT")),
        "catchment (sq.km)": safe_value(record.get("CATCHMENT")),
        "station_type (0=dam, 5=barrage)": stn_type,
        "source_agency": SOURCE_AGENCY,
        "source_url": SOURCE_BASE_URL,
        "last_updated": fetched_at,
    }


def build_snapshot_row(record: dict, stn_type: int) -> dict:
    rid = build_reservoir_id(record, stn_type)
    return {
        "reservoir_id": rid,
        "reservoir_name": record.get("STATION_FULLNAME", "") or record.get("STATION_NAME", ""),
        "date": extract_obs_date(record),
        "DATE_UPDATE (source observation timestamp, local time)": safe_value(record.get("DATE_UPDATE")),
        "WATER_LEVEL (water level, m)": safe_value(record.get("WATER_LEVEL")),
        "STORAGE (current storage, mcm / 10^6 m^3)": safe_value(record.get("STORAGE")),
        "STORAGE_PERCENT (percent full, %)": safe_value(record.get("STORAGE_PERCENT")),
        "RELEASE_MLD (release / outflow, mld = 10^6 liters per day)": safe_value(record.get("RELEASE_MLD")),
        "SPILL (spillway release, mld)": safe_value(record.get("SPILL")),
        "RAIN (rainfall, mm)": safe_value(record.get("RAIN")),
        "CUMM_RAIN (cumulative rainfall, mm)": safe_value(record.get("CUMM_RAIN")),
        "RESIDUAL (residual flow, mld)": safe_value(record.get("RESIDUAL")),
        "RESERVOIR (reservoir flow, mld)": safe_value(record.get("RESERVOIR")),
        "CONTROL_VOLUME (control volume, mcm)": safe_value(record.get("CONTROL_VOLUME")),
    }


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"[SAVE] {path}  rows={len(rows)}")


def save_raw_json(data: Any, prefix: str, poll_stamp: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{prefix}_{poll_stamp}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {path}")
    return path


def merge_metadata(old_rows: list[dict], new_rows: list[dict]) -> list[dict]:
    by_id = {r["reservoir_id"]: dict(r) for r in old_rows}
    for nr in new_rows:
        rid = nr["reservoir_id"]
        if rid in by_id:
            by_id[rid].update({k: v for k, v in nr.items() if v != ""})
        else:
            by_id[rid] = nr
    return list(by_id.values())


def read_existing_metadata() -> list[dict]:
    path = METADATA_DIR / "malaysia_luas_reservoirs.csv"
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> int:
    print(f"[INFO] OUTPUT_DIR = {OUTPUT_DIR}")
    for d in (METADATA_DIR, DAILY_DIR, RAW_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            )
        }
    )
    try:
        session.get(REFERER, timeout=TIMEOUT)
    except Exception as e:
        print(f"[WARN] warmup failed: {e}")

    fetched_at = now_stamp()
    poll_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    dams = fetch_json(DAM_URL, session)
    barrages = fetch_json(BARRAGE_URL, session)
    print(f"[INFO] fetched dams={len(dams)}, barrages={len(barrages)}")

    save_raw_json(dams, "damstation", poll_stamp)
    save_raw_json(barrages, "barrage", poll_stamp)

    new_meta_rows: list[dict] = []
    for rec in dams:
        new_meta_rows.append(build_metadata_row(rec, stn_type=0, fetched_at=fetched_at))
    for rec in barrages:
        new_meta_rows.append(build_metadata_row(rec, stn_type=5, fetched_at=fetched_at))

    existing = read_existing_metadata()
    merged = merge_metadata(existing, new_meta_rows)
    write_csv(METADATA_DIR / "malaysia_luas_reservoirs.csv", METADATA_COLUMNS, merged)

    all_snapshot_rows: list[dict] = []
    for rec in dams:
        all_snapshot_rows.append(build_snapshot_row(rec, stn_type=0))
    for rec in barrages:
        all_snapshot_rows.append(build_snapshot_row(rec, stn_type=5))

    by_date: dict[str, list[dict]] = {}
    for row in all_snapshot_rows:
        by_date.setdefault(row["date"], []).append(row)

    for date, rows in by_date.items():
        path = DAILY_DIR / f"malaysia_luas_timeseries_{date}.csv"
        if path.exists():
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                existing_rows = {r["reservoir_id"]: r for r in csv.DictReader(f)}
            for r in rows:
                existing_rows[r["reservoir_id"]] = r
            rows = list(existing_rows.values())
        write_csv(path, SNAPSHOT_COLUMNS, rows)

    summary = {
        "run_time": fetched_at,
        "source": {"dam_url": DAM_URL, "barrage_url": BARRAGE_URL},
        "counts": {"dams": len(dams), "barrages": len(barrages)},
        "snapshot_files": sorted(
            [str(DAILY_DIR / f"malaysia_luas_timeseries_{d}.csv") for d in by_date]
        ),
        "metadata_file": str(METADATA_DIR / "malaysia_luas_reservoirs.csv"),
    }
    summary_path = RUN_LOG_DIR / f"{poll_stamp}_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {summary_path}")

    print("[DONE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

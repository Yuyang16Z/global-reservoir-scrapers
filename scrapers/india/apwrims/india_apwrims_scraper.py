"""
India APWRIMS reservoir scraper.

Source: https://apwrims.ap.gov.in/  (in-situ gauge data, NOT remote sensing)

Covers ~118 reservoirs in Andhra Pradesh, Telangana, and the wider Krishna
basin. Data is fed by physical water-level sensors and operator-reported
gate / canal / spillway / power-house flow logs.

Endpoints used:
    GET /api/reservoir/map/all                              -> reservoir list
    GET /api/v2/reservoir/extension/<uuid>                  -> static metadata
    GET /api/v2/reservoir/getlastnvalues/<uuid>/<n>         -> last 3-4 obs

The lastnvalues endpoint is server-capped at ~3 records regardless of n; the
daily cron is designed around this so observations accumulate over time.

Per project schema.md the scraper does NO unit conversion — TMC and cusec
are kept as the source emits them, with the unit annotated in the column
name.

Layout (matches project schema):
    data/india/apwrims/
        metadata/india_apwrims_reservoirs.csv
        timeseries/daily/india_apwrims_timeseries_YYYY-MM-DD.csv
        run_logs/<timestamp>_summary.json
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

API_BASE = "https://apwrims.ap.gov.in"
SOURCE_AGENCY = "APWRIMS"
SOURCE_URL = "https://apwrims.ap.gov.in/"
TIMEOUT = 25
RETRIES = 3
RETRY_BACKOFF = 2.0
PER_REQUEST_DELAY = 0.25
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

OUT_BASE = Path(os.environ.get("OUTPUT_DIR") or "data/india/apwrims").resolve()
METADATA_DIR = OUT_BASE / "metadata"
DAILY_DIR = OUT_BASE / "timeseries" / "daily"
RUN_LOG_DIR = OUT_BASE / "run_logs"

# Source units kept as-is per schema.md "不要换算":
#   storage / capacity      -> TMC   (Thousand Million Cubic feet)
#   inflow / outflow        -> cusec (Cubic feet per second)
#   level                   -> m (masl)
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
    "capacity_total (TMC)",
    "dead_storage (TMC)",
    "frl (m)",
    "dam_height (m)",
    "year_built",
    "main_use",
    "data_type",
    "source_agency",
    "source_url",
    "last_updated",
    "detail_slug",
    "location",
    "data_period_start",
    "data_period_end",
]

SNAPSHOT_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "level (water level, m)",
    "storage (live storage, TMC)",
    "storage_pct (% of design capacity)",
    "inflow (cusec)",
    "outflow (cusec)",
]


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def slugify(s: str) -> str:
    out = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return out or "unknown"


def reservoir_id_for(name: str, uuid: str) -> str:
    return f"IN_APWRIMS_{slugify(name)}_{uuid[:8]}"


def parse_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r"-?\d+(\.\d+)?", s)
    return float(m.group(0)) if m else None


def fmt(v: float | None, digits: int = 3) -> str:
    if v is None:
        return ""
    if digits <= 0:
        return f"{int(round(v))}"
    rounded = round(v, digits)
    s = f"{rounded:.{digits}f}".rstrip("0").rstrip(".")
    return s if s else "0"


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def fetch_json(session: requests.Session, path: str) -> Any:
    url = API_BASE + path
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
    raise RuntimeError(f"GET {url} failed after {RETRIES} attempts: {last_err}")


def list_reservoirs(session: requests.Session) -> list[dict]:
    data = fetch_json(session, "/api/reservoir/map/all")
    out: list[dict] = []
    for group in data.get("locationTypeLocationList", []):
        if group.get("type") != "RESERVOIR":
            continue
        for item in group.get("locationList", []):
            ld = item.get("locationData", {})
            ext = ld.get("extensions") or {}
            out.append({
                "uuid": ld.get("locationUuid"),
                "name": (ld.get("displayName") or "").strip(),
                "lat": parse_float(ext.get("latitude")),
                "lon": parse_float(ext.get("longitude")),
            })
    return [r for r in out if r["uuid"] and r["name"]]


def fetch_extension(session: requests.Session, uuid: str) -> dict:
    try:
        return fetch_json(session, f"/api/v2/reservoir/extension/{uuid}") or {}
    except Exception:
        return {}


def fetch_last_observations(session: requests.Session, uuid: str, n: int = 10) -> dict:
    try:
        data = fetch_json(session, f"/api/v2/reservoir/getlastnvalues/{uuid}/{n}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def parse_observations(raw: dict) -> list[dict]:
    """{ts_ms: payload} -> list of one-row-per-UTC-date observations."""
    by_date: dict[str, dict] = {}
    for ts_ms_str, payload in raw.items():
        try:
            ts_ms = int(ts_ms_str)
        except ValueError:
            continue
        if not isinstance(payload, dict):
            continue
        d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date().isoformat()
        existing = by_date.get(d)
        if existing is None or ts_ms > existing["_ts_ms"]:
            by_date[d] = {
                "_ts_ms": ts_ms,
                "date": d,
                "level": parse_float(payload.get("level")),
                "storage_tmc": parse_float(payload.get("storage")),
                "inflow_cusec": parse_float(payload.get("inflow")),
                "outflow_cusec": parse_float(payload.get("outflow")),
            }
    return sorted(by_date.values(), key=lambda r: r["date"])


def build_metadata_row(res: dict, ext: dict, latest: dict | None) -> dict:
    name = res["name"]
    uuid = res["uuid"]
    cap_tmc = parse_float(ext.get("designcapacity"))
    dead_tmc = parse_float(ext.get("deadstorage"))
    return {
        "reservoir_id": reservoir_id_for(name, uuid),
        "reservoir_name": name,
        "reservoir_name_en": name,                  # APWRIMS names are already English
        "country": "India",
        "admin_unit": "Andhra Pradesh / Telangana",
        "river": (ext.get("river") or "").strip(),
        "basin": (ext.get("basin") or "").strip(),
        "lat": fmt(res.get("lat"), 6),
        "lon": fmt(res.get("lon"), 6),
        "capacity_total (TMC)": fmt(cap_tmc, 3),
        "dead_storage (TMC)": fmt(dead_tmc, 3),
        "frl (m)": fmt(parse_float(ext.get("fullreservoirlevel")), 3),
        "dam_height (m)": "",                       # not in APWRIMS
        "year_built": "",                           # not in APWRIMS
        "main_use": (ext.get("projectpurpose") or "").strip(),
        "data_type": "in_situ",
        "source_agency": SOURCE_AGENCY,
        "source_url": SOURCE_URL,
        "last_updated": now_stamp(),
        "detail_slug": uuid,
        "location": (ext.get("districtsbenefitted") or "").strip(),
        "data_period_start": "",                    # filled from accumulated observations
        "data_period_end": latest.get("date", "") if latest else "",
    }


def build_snapshot_row(reservoir_id: str, name: str, obs: dict, cap_tmc: float | None) -> dict:
    s_tmc = obs.get("storage_tmc")
    pct = ""
    if s_tmc is not None and cap_tmc and cap_tmc > 0:
        pct = fmt(s_tmc / cap_tmc * 100, 2)
    return {
        "reservoir_id": reservoir_id,
        "reservoir_name": name,
        "date": obs["date"],
        "level (water level, m)": fmt(obs.get("level"), 3),
        "storage (live storage, TMC)": fmt(s_tmc, 4),
        "storage_pct (% of design capacity)": pct,
        "inflow (cusec)": fmt(obs.get("inflow_cusec"), 2),
        "outflow (cusec)": fmt(obs.get("outflow_cusec"), 2),
    }


def write_metadata(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=METADATA_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in METADATA_COLUMNS})


def merge_snapshot(new_rows: list[dict], path: Path) -> int:
    """Idempotent merge by reservoir_id; preserves existing rows for the date."""
    existing: dict[str, dict] = {}
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                rid = row.get("reservoir_id")
                if rid:
                    existing[rid] = row
    for row in new_rows:
        existing[row["reservoir_id"]] = row
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SNAPSHOT_COLUMNS)
        w.writeheader()
        for row in sorted(existing.values(), key=lambda r: r["reservoir_id"]):
            w.writerow({c: row.get(c, "") for c in SNAPSHOT_COLUMNS})
    return len(existing)


def merge_data_period(metadata_rows: list[dict]) -> None:
    """Backfill data_period_start by scanning existing daily snapshots."""
    if not DAILY_DIR.exists():
        return
    earliest_by_id: dict[str, str] = {}
    for f in DAILY_DIR.glob("india_apwrims_timeseries_*.csv"):
        try:
            d = f.stem.rsplit("_", 1)[-1]
            datetime.strptime(d, "%Y-%m-%d")
        except (ValueError, IndexError):
            continue
        with f.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                rid = row.get("reservoir_id")
                if not rid:
                    continue
                cur = earliest_by_id.get(rid)
                if cur is None or d < cur:
                    earliest_by_id[rid] = d
    for r in metadata_rows:
        rid = r["reservoir_id"]
        if rid in earliest_by_id:
            r["data_period_start"] = earliest_by_id[rid]


def run() -> dict:
    session = make_session()
    print(f"[{now_stamp()}] Listing reservoirs from APWRIMS...", flush=True)
    reservoirs = list_reservoirs(session)
    print(f"  found {len(reservoirs)} reservoirs", flush=True)

    metadata_rows: list[dict] = []
    snapshot_rows_by_date: dict[str, list[dict]] = {}
    no_data_count = 0
    error_count = 0

    for i, res in enumerate(reservoirs, 1):
        try:
            ext = fetch_extension(session, res["uuid"])
            time.sleep(PER_REQUEST_DELAY)
            obs_raw = fetch_last_observations(session, res["uuid"], n=10)
            obs_list = parse_observations(obs_raw)
            cap_tmc = parse_float(ext.get("designcapacity"))
            latest = obs_list[-1] if obs_list else None
            metadata_rows.append(build_metadata_row(res, ext, latest))

            rid = reservoir_id_for(res["name"], res["uuid"])
            if not obs_list:
                no_data_count += 1
            for obs in obs_list:
                row = build_snapshot_row(rid, res["name"], obs, cap_tmc)
                snapshot_rows_by_date.setdefault(obs["date"], []).append(row)

            if i % 20 == 0 or i == len(reservoirs):
                print(f"  [{i}/{len(reservoirs)}] {res['name']}", flush=True)
        except Exception as e:
            error_count += 1
            print(f"  ERROR on {res.get('name')}: {e}", file=sys.stderr, flush=True)
        time.sleep(PER_REQUEST_DELAY)

    metadata_path = METADATA_DIR / "india_apwrims_reservoirs.csv"
    snapshot_paths: list[Path] = []
    for d, rows in sorted(snapshot_rows_by_date.items()):
        path = DAILY_DIR / f"india_apwrims_timeseries_{d}.csv"
        total_after = merge_snapshot(rows, path)
        snapshot_paths.append(path)
        print(f"[{now_stamp()}] {d}: merged {len(rows)} rows -> {path.name} (total {total_after})", flush=True)

    # Backfill data_period_start by reading existing snapshots before writing metadata
    merge_data_period(metadata_rows)
    write_metadata(metadata_rows, metadata_path)
    print(f"[{now_stamp()}] Wrote metadata: {metadata_path} ({len(metadata_rows)} rows)", flush=True)

    return {
        "ran_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source_url": SOURCE_URL,
        "status": "success" if error_count == 0 else "partial",
        "reservoir_count": len(reservoirs),
        "metadata_rows": len(metadata_rows),
        "no_data_count": no_data_count,
        "error_count": error_count,
        "snapshot_dates": sorted(snapshot_rows_by_date.keys()),
        "metadata_path": str(metadata_path.relative_to(OUT_BASE.parent.parent)) if OUT_BASE.parent.parent in metadata_path.parents else str(metadata_path),
        "snapshot_paths": [str(p.relative_to(OUT_BASE.parent.parent)) if OUT_BASE.parent.parent in p.parents else str(p) for p in snapshot_paths],
    }


def main() -> int:
    summary: dict
    try:
        summary = run()
    except Exception as e:
        traceback.print_exc()
        summary = {
            "ran_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "source_url": SOURCE_URL,
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUN_LOG_DIR / f"{datetime.now(tz=timezone.utc).date().isoformat()}_summary.json"
    log_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[{now_stamp()}] Run log: {log_path}", flush=True)
    return 0 if summary.get("status") in ("success", "partial") else 1


if __name__ == "__main__":
    sys.exit(main())

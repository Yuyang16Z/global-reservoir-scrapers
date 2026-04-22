"""
Malaysia — DID Sarawak iHydro river + rainfall gauge scraper.

Source: https://ihydro.sarawak.gov.my/iHydro/en/map/maps.jsp
Coverage: ~271 stations across all Sarawak divisions (Rainfall, Combine, Water, IG types).

IMPORTANT: These are **RIVER / RAINFALL / IG stations**, NOT reservoir stations.
Sarawak's big hydro reservoirs (Bakun, Murum, Batang Ai) are NOT on iHydro — they're
operated by Sarawak Energy / SEB and do not publish live data publicly.

Why in the repo despite not being reservoirs:
- Discharge / river-stage data is a secondary layer of the Global Reservoir Dataset
  (upstream/downstream gauges are useful for cross-referencing reservoir inflow/outflow).
- Also provides basin-level rainfall as a co-variable.

One HTTP request gets it all: the maps.jsp page embeds the full station list as a
JS array in a hidden <input id="xml">, including lat/lon, division, river basin,
alert thresholds, latest water level, latest rainfall, and the observation timestamp.
The paginated `latest-waterlevel.jsp` has fewer fields and needs 8 round-trips, so
we prefer maps.jsp.

Output layout:
  <OUTPUT_DIR>/
    metadata/malaysia_sarawak_rivers_stations.csv          # one row per station (static attrs + lat/lon)
    timeseries/daily/malaysia_sarawak_rivers_timeseries_YYYY-MM-DD.csv
    raw/maps_YYYYMMDD_HHMMSS.html
    run_logs/<stamp>_summary.json
"""

from __future__ import annotations

import csv
import html
import json
import os
import re
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
    OUTPUT_DIR = BASE_DIR / "malaysia_sarawak_rivers_outputs"

METADATA_DIR = OUTPUT_DIR / "metadata"
DAILY_DIR = OUTPUT_DIR / "timeseries" / "daily"
RAW_DIR = OUTPUT_DIR / "raw"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

HOST = "https://ihydro.sarawak.gov.my"
MAP_URL = f"{HOST}/iHydro/en/map/maps.jsp"
TIMEOUT = 45

SOURCE_AGENCY = "DID Sarawak (iHydro)"
SOURCE_BASE_URL = f"{HOST}/"


METADATA_COLUMNS = [
    "station_id",
    "station_name",
    "station_type",
    "country",
    "admin_unit",
    "division",
    "river_basin",
    "lat",
    "lon",
    "normal_level (m)",
    "alert_level (m)",
    "warning_level (m)",
    "danger_level (m)",
    "wl_datum (based on)",
    "source_agency",
    "source_url",
    "last_updated",
]

SNAPSHOT_COLUMNS = [
    "station_id",
    "station_name",
    "date",
    "observation_timestamp (source, local time)",
    "water_level (m)",
    "wl_status",
    "daily_rainfall (mm)",
    "latest_rainfall (mm)",
    "rainfall_status",
]


ROW_PATTERN = re.compile(
    r"^\[(\d+),'([^']*)','([^']*)',\s*([\d.\-]+)\s*,\s*([\d.\-]+)\s*,'([^']*)','([^']*)','(.*)'\],?\s*$",
    re.DOTALL,
)

XML_INPUT_PATTERN = re.compile(
    r'<input\s+type="hidden"\s+name="xml"\s+id="xml"\s+value="(.*?)"\s*/?>',
    re.DOTALL,
)

STATION_ID_FROM_TOOLTIP = re.compile(r"\?station=(\d+)")
TIME_TAKEN_PATTERN = re.compile(r"Time Taken\s*:\s*([0-9]{1,2}-[0-9]{1,2}-[0-9]{4}\s+[0-9]{1,2}:[0-9]{2})")
NORMAL_LEVEL_PATTERN = re.compile(r"Normal Level\s*:\s*([\d.]+)\s*m")
ALERT_LEVEL_PATTERN = re.compile(r"Alert Level\s*:\s*([\d.]+)\s*m")
WARNING_LEVEL_PATTERN = re.compile(r"Warning Level\s*:\s*([\d.]+)\s*m")
DANGER_LEVEL_PATTERN = re.compile(r"Danger Level\s*:\s*([\d.]+)\s*m")
LATEST_WL_PATTERN = re.compile(r"Latest WL\s*:\s*([\d.\-]+)\s*m")
WL_STATUS_PATTERN = re.compile(r"WL Status\s*:\s*([^<]+?)(?:<br>|$)")
DAILY_RF_PATTERN = re.compile(r"Daily RF\s*:\s*([\d.\-]+)\s*mm")
LATEST_RF_PATTERN = re.compile(r"Latest RF\s*:\s*([\d.\-]+)\s*mm")
RF_STATUS_PATTERN = re.compile(r"(?<!WL )(?:RF )?Status\s*:\s*([^<]+?)(?:<br>|$)")
WL_DATUM_PATTERN = re.compile(r"\(WL Based on ([^)]+)\)")


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe(v: str | None) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "null" else s


def first(pat: re.Pattern, text: str) -> str:
    m = pat.search(text)
    return m.group(1).strip() if m else ""


def parse_observation_date(ts: str) -> str:
    """
    Source format 'DD-MM-YYYY HH:MM' -> 'YYYY-MM-DD'.
    Fallback: today (scrape-time local date).
    """
    if ts:
        try:
            return datetime.strptime(ts.split(" ")[0], "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d")


def parse_xml_value(html_text: str) -> str:
    m = XML_INPUT_PATTERN.search(html_text)
    if not m:
        raise RuntimeError("xml hidden input not found in maps.jsp HTML")
    # HTML-decode (the value attribute contains &lt; / &gt; etc in the tooltip)
    return html.unescape(m.group(1))


def iter_rows(xml_body: str):
    """
    xml_body is a pseudo-JSON JS array. We scan line-by-line; each valid row line starts with '['
    and ends with '],' (last one with ']'). Tooltip HTML may contain embedded commas and quotes
    but is wrapped in the last single-quoted field.
    """
    for line in xml_body.splitlines():
        line = line.strip()
        if not line.startswith("["):
            continue
        m = ROW_PATTERN.match(line)
        if not m:
            continue
        idx, name, stype, lat, lon, rf_status, wl_status, tooltip = m.groups()
        yield {
            "idx": idx,
            "name_combined": name,
            "station_type": stype,
            "lat": lat,
            "lon": lon,
            "rf_status_top": rf_status,
            "wl_status_top": wl_status,
            "tooltip": tooltip,
        }


def parse_station(row: dict, fetched_at: str) -> tuple[dict, dict]:
    """Return (metadata_row, snapshot_row)."""
    tooltip = row["tooltip"]
    sid = first(STATION_ID_FROM_TOOLTIP, tooltip)
    if not sid:
        return {}, {}
    # Strip "-<Type>" suffix from combined name to get the base station name
    base_name = re.sub(r"-(Combine|Rainfall|Water|IG)$", "", row["name_combined"])

    # Type-dependent fields
    normal_level = first(NORMAL_LEVEL_PATTERN, tooltip)
    alert_level = first(ALERT_LEVEL_PATTERN, tooltip)
    warning_level = first(WARNING_LEVEL_PATTERN, tooltip)
    danger_level = first(DANGER_LEVEL_PATTERN, tooltip)
    latest_wl = first(LATEST_WL_PATTERN, tooltip)
    wl_status = first(WL_STATUS_PATTERN, tooltip)
    daily_rf = first(DAILY_RF_PATTERN, tooltip)
    latest_rf = first(LATEST_RF_PATTERN, tooltip)
    # Rainfall-only stations use plain "Status :" instead of "RF Status :". Handle either.
    rf_status = first(RF_STATUS_PATTERN, tooltip)
    wl_datum = first(WL_DATUM_PATTERN, tooltip)
    time_taken = first(TIME_TAKEN_PATTERN, tooltip)

    division = ""
    basin = ""
    dm = re.search(r"Division:\s*([^<]+?)(?:<br>|$)", tooltip)
    if dm:
        division = dm.group(1).strip()
    bm = re.search(r"River Basin:\s*([^<]+?)(?:<br>|$)", tooltip)
    if bm:
        basin = bm.group(1).strip()

    metadata = {
        "station_id": f"MY_DIDSRWK_{sid}",
        "station_name": base_name,
        "station_type": row["station_type"],
        "country": "Malaysia",
        "admin_unit": "Sarawak",
        "division": division,
        "river_basin": basin,
        "lat": safe(row["lat"]),
        "lon": safe(row["lon"]),
        "normal_level (m)": normal_level,
        "alert_level (m)": alert_level,
        "warning_level (m)": warning_level,
        "danger_level (m)": danger_level,
        "wl_datum (based on)": wl_datum,
        "source_agency": SOURCE_AGENCY,
        "source_url": SOURCE_BASE_URL,
        "last_updated": fetched_at,
    }

    snapshot = {
        "station_id": f"MY_DIDSRWK_{sid}",
        "station_name": base_name,
        "date": parse_observation_date(time_taken),
        "observation_timestamp (source, local time)": time_taken,
        "water_level (m)": latest_wl,
        "wl_status": wl_status,
        "daily_rainfall (mm)": daily_rf,
        "latest_rainfall (mm)": latest_rf,
        "rainfall_status": rf_status,
    }
    return metadata, snapshot


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"[SAVE] {path}  rows={len(rows)}")


def merge_metadata(old_rows: list[dict], new_rows: list[dict]) -> list[dict]:
    by_id = {r["station_id"]: dict(r) for r in old_rows}
    for nr in new_rows:
        sid = nr["station_id"]
        if sid in by_id:
            by_id[sid].update({k: v for k, v in nr.items() if v != ""})
        else:
            by_id[sid] = nr
    return list(by_id.values())


def read_existing_metadata() -> list[dict]:
    path = METADATA_DIR / "malaysia_sarawak_rivers_stations.csv"
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> int:
    print(f"[INFO] OUTPUT_DIR = {OUTPUT_DIR}")
    for d in (METADATA_DIR, DAILY_DIR, RAW_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    fetched_at = now_stamp()
    poll_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    resp = requests.get(
        MAP_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            )
        },
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    page = resp.text

    raw_path = RAW_DIR / f"maps_{poll_stamp}.html"
    raw_path.write_text(page, encoding="utf-8")
    print(f"[SAVE] {raw_path}")

    xml_body = parse_xml_value(page)

    new_meta: list[dict] = []
    snapshots: list[dict] = []
    skipped = 0
    for row in iter_rows(xml_body):
        meta, snap = parse_station(row, fetched_at)
        if not meta:
            skipped += 1
            continue
        new_meta.append(meta)
        snapshots.append(snap)
    print(f"[INFO] parsed {len(new_meta)} stations (skipped={skipped})")

    if not new_meta:
        print("[ERROR] zero stations parsed — page format may have changed. Check raw/.")
        return 2

    merged = merge_metadata(read_existing_metadata(), new_meta)
    write_csv(METADATA_DIR / "malaysia_sarawak_rivers_stations.csv", METADATA_COLUMNS, merged)

    # Group snapshots by observation date (source timestamp); each date -> one snapshot file.
    by_date: dict[str, list[dict]] = {}
    for snap in snapshots:
        by_date.setdefault(snap["date"], []).append(snap)

    for date, rows in by_date.items():
        path = DAILY_DIR / f"malaysia_sarawak_rivers_timeseries_{date}.csv"
        if path.exists():
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                existing_rows = {r["station_id"]: r for r in csv.DictReader(f)}
            for r in rows:
                existing_rows[r["station_id"]] = r
            rows = list(existing_rows.values())
        write_csv(path, SNAPSHOT_COLUMNS, rows)

    summary: dict[str, Any] = {
        "run_time": fetched_at,
        "source_url": MAP_URL,
        "station_count": len(new_meta),
        "skipped": skipped,
        "snapshot_files": sorted(
            [str(DAILY_DIR / f"malaysia_sarawak_rivers_timeseries_{d}.csv") for d in by_date]
        ),
        "metadata_file": str(METADATA_DIR / "malaysia_sarawak_rivers_stations.csv"),
    }
    summary_path = RUN_LOG_DIR / f"{poll_stamp}_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SAVE] {summary_path}")

    print("[DONE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

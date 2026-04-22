"""Thailand RID (Royal Irrigation Department) reservoir scraper.

Hits the two JSON endpoints backing https://app.rid.go.th/reservoir/ :
- /api/dams           -> ~35 large dams
- /api/rsvmiddles     -> ~448 medium reservoirs (status=1 = active)

Default: fetch "today" and "yesterday" (Bangkok, UTC+8). Designed to run on a cron.
Set THAILAND_START_DATE / THAILAND_END_DATE (YYYY-MM-DD) to backfill a range.

Outputs under OUTPUT_DIR (default: this script's folder / thailand_rid_outputs):
  metadata/thailand_reservoirs.csv                   static per-reservoir info
  timeseries/daily/thailand_timeseries_<YYYY-MM-DD>.csv   slim per-day snapshot
  raw/dams/<YYYY-MM-DD>.json                         raw API payload (for audit)
  raw/rsvmiddles/<YYYY-MM-DD>.json
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
from typing import Any, Iterable

import requests


DAMS_URL = "https://app.rid.go.th/reservoir/api/dams"
MIDDLES_URL = "https://app.rid.go.th/reservoir/api/rsvmiddles"
SOURCE_URL = "https://app.rid.go.th/reservoir/"
SOURCE_AGENCY = "RID"

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://app.rid.go.th",
    "Referer": "https://app.rid.go.th/reservoir/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}

BANGKOK_TZ = timezone(timedelta(hours=7))

# Unit convention for RID: storage/flow volumes are reported in million m^3.
# Units go in column names per schema.md; no unit conversion at scrape time.
TS_COLUMNS: list[tuple[str, str]] = [
    ("reservoir_id", "reservoir_id"),
    ("reservoir_name", "reservoir_name"),
    ("date", "date"),
    ("source_type", "source_type"),
    ("storage_current (million m^3)", "storage_current"),
    ("storage_pct_current (%)", "storage_pct_current"),
    ("storage_prev_year_same_day (million m^3)", "storage_prev_year_same_day"),
    ("storage_pct_prev_year_same_day (%)", "storage_pct_prev_year_same_day"),
    ("storage_jan1_current_year (million m^3)", "storage_jan1_current_year"),
    ("storage_jan1_prev_year (million m^3)", "storage_jan1_prev_year"),
    ("inflow_daily (million m^3/day)", "inflow_daily"),
    ("inflow_ytd (million m^3)", "inflow_ytd"),
    ("outflow_daily (million m^3/day)", "outflow_daily"),
    ("outflow_ytd (million m^3)", "outflow_ytd"),
    ("usable_water_current (million m^3)", "usable_water_current"),
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
    "capacity_total (million m^3)",
    "storage_capacity (million m^3)",
    "usable_capacity (million m^3)",
    "dead_storage (million m^3)",
    "avg_year_inflow (million m^3/year)",
    "source_type",
    "province",
    "rid_office",
    "project_name",
    "source_agency",
    "source_url",
    "last_updated",
]


def clean_value(v: Any) -> Any:
    if isinstance(v, str):
        v = v.strip()
        if v in {"", "-", "- ", " -", " - "}:
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


def post_json(session: requests.Session, url: str, payload: dict, timeout: int = 60) -> dict:
    resp = session.post(url, headers=HEADERS, data=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _dams_payload(date_str: str) -> dict:
    return {"date": date_str, "region": "", "percent": "", "percent_from": "", "percent_to": ""}


def _middles_payload(date_str: str) -> dict:
    return {
        "date": date_str, "region": "", "percent": "",
        "percent_from": "", "percent_to": "", "status": "1",
    }


def flatten_large(data: dict) -> list[dict]:
    rows: list[dict] = []
    for region_block in data.get("regions", []):
        region_name = clean_value(region_block.get("region_name"))
        for dam in region_block.get("dams", []):
            rows.append({
                "source_type": "large",
                "reservoir_id": clean_value(dam.get("DAM_ID")),
                "reservoir_name": clean_value(dam.get("DAM_Name")),
                "date": clean_value(dam.get("DMD_Date")),
                "region_name_th": region_name,
                "region_name_en": None,
                "province": None,
                "rid_office": None,
                "project_name": None,
                "lat": try_float(dam.get("DAM_Lat")),
                "lon": try_float(dam.get("DAM_Lon")),
                "capacity_total": try_float(dam.get("DAM_QMax")),
                "storage_capacity": try_float(dam.get("DAM_QStore")),
                "usable_capacity": try_float(dam.get("DAM_QUsage")),
                "dead_storage": try_float(dam.get("DUL_Useless")),
                "avg_year_inflow": try_float(dam.get("AVG_Year_Inflow")),
                "storage_current": try_float(dam.get("DMD_QUse")),
                "storage_pct_current": try_float(dam.get("PERCENT_DMD_QUse")),
                "storage_prev_year_same_day": try_float(dam.get("DMD_QUse_prev")),
                "storage_pct_prev_year_same_day": try_float(dam.get("PERCENT_DMD_QUse_prev")),
                "storage_jan1_current_year": try_float(dam.get("Jan_Curr")),
                "storage_jan1_prev_year": try_float(dam.get("Jan_Prev")),
                "inflow_daily": try_float(dam.get("DMD_Inflow")),
                "inflow_ytd": try_float(dam.get("SUM_Inflow")),
                "outflow_daily": try_float(dam.get("DMD_Outflow")),
                "outflow_ytd": try_float(dam.get("SUM_Outflow")),
                "usable_water_current": None,
            })
    return rows


def flatten_middle(data: dict) -> list[dict]:
    rows: list[dict] = []
    for region_block in data.get("region", []):
        region_name_th = clean_value(region_block.get("region_name_th"))
        region_name_en = clean_value(region_block.get("region_name_en"))
        for r in region_block.get("reservoir", []):
            rows.append({
                "source_type": "middle",
                "reservoir_id": clean_value(r.get("cresv")),
                "reservoir_name": clean_value(r.get("nresv")),
                "date": clean_value(r.get("date")),
                "region_name_th": region_name_th,
                "region_name_en": region_name_en,
                "province": clean_value(r.get("tprov")),
                "rid_office": clean_value(r.get("rid")),
                "project_name": clean_value(r.get("project_name")),
                "lat": try_float(r.get("cresv_lat")),
                "lon": try_float(r.get("cresv_lng")),
                "capacity_total": try_float(r.get("cap_resv")),
                "storage_capacity": None,
                "usable_capacity": None,
                "dead_storage": try_float(r.get("low_qdisc")),
                "avg_year_inflow": None,
                "storage_current": try_float(r.get("qdisc_curr")),
                "storage_pct_current": try_float(r.get("percent_resv_curr")),
                "storage_prev_year_same_day": try_float(r.get("qdisc_prev")),
                "storage_pct_prev_year_same_day": try_float(r.get("percent_resv_prev")),
                "storage_jan1_current_year": try_float(r.get("jan_info")),
                "storage_jan1_prev_year": None,
                "inflow_daily": try_float(r.get("q_info")),
                "inflow_ytd": None,
                "outflow_daily": try_float(r.get("q_outfo")),
                "outflow_ytd": None,
                "usable_water_current": try_float(r.get("water_workable")),
            })
    return rows


def write_timeseries_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([col for col, _ in TS_COLUMNS])
        for r in rows:
            writer.writerow([r.get(key) if r.get(key) is not None else "" for _, key in TS_COLUMNS])


def save_raw_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _row_to_meta(r: dict, now_str: str) -> dict:
    return {
        "reservoir_id": r.get("reservoir_id"),
        "reservoir_name": r.get("reservoir_name"),
        "reservoir_name_en": None,
        "country": "Thailand",
        "admin_unit": r.get("region_name_en") or r.get("region_name_th"),
        "river": None,
        "basin": None,
        "lat": r.get("lat"),
        "lon": r.get("lon"),
        "capacity_total (million m^3)": r.get("capacity_total"),
        "storage_capacity (million m^3)": r.get("storage_capacity"),
        "usable_capacity (million m^3)": r.get("usable_capacity"),
        "dead_storage (million m^3)": r.get("dead_storage"),
        "avg_year_inflow (million m^3/year)": r.get("avg_year_inflow"),
        "source_type": r.get("source_type"),
        "province": r.get("province"),
        "rid_office": r.get("rid_office"),
        "project_name": r.get("project_name"),
        "source_agency": SOURCE_AGENCY,
        "source_url": SOURCE_URL,
        "last_updated": now_str,
    }


def upsert_metadata(metadata_path: Path, fresh_rows: list[dict],
                    raw_dams_dir: Path | None = None,
                    raw_middles_dir: Path | None = None) -> int:
    """Merge freshly-fetched rows into existing metadata CSV.

    Keeps existing entries for reservoir_ids not seen this run. Re-flattens raw
    JSONs if available (useful during backfill); otherwise works purely from
    in-memory fresh rows + what's already in the CSV.
    """
    # Date-only to avoid churning `last_updated` twice a day.
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    existing: "OrderedDict[str, dict]" = OrderedDict()
    if metadata_path.exists():
        try:
            with open(metadata_path, encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    rid = row.get("reservoir_id")
                    if rid:
                        existing[rid] = row
        except Exception as e:
            print(f"[WARN] failed to read existing metadata {metadata_path}: {e}", file=sys.stderr)

    # If raw JSONs exist (backfill case), include them as extra source rows.
    all_rows = list(fresh_rows)
    for d, flat in ((raw_dams_dir, flatten_large), (raw_middles_dir, flatten_middle)):
        if not d or not d.exists():
            continue
        for p in sorted(d.glob("*.json")):
            try:
                all_rows.extend(flat(json.loads(p.read_text(encoding="utf-8"))))
            except Exception as e:
                print(f"[WARN] failed to parse {p}: {e}", file=sys.stderr)

    for r in all_rows:
        rid = r.get("reservoir_id")
        if not rid:
            continue
        existing[rid] = _row_to_meta(r, now_str)

    if not existing:
        return 0

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=META_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing.values())
    return len(existing)


def target_dates() -> list[str]:
    start = os.environ.get("THAILAND_START_DATE")
    end = os.environ.get("THAILAND_END_DATE")
    if start and end:
        s = datetime.strptime(start, "%Y-%m-%d").date()
        e = datetime.strptime(end, "%Y-%m-%d").date()
        out = []
        d = s
        while d <= e:
            out.append(d.isoformat())
            d += timedelta(days=1)
        return out
    # Default: today + yesterday Bangkok time (RID updates early morning BKK; give a fallback).
    today_bkk = datetime.now(BANGKOK_TZ).date()
    return [(today_bkk - timedelta(days=1)).isoformat(), today_bkk.isoformat()]


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    output_dir = Path(os.environ.get("OUTPUT_DIR", script_dir / "thailand_rid_outputs"))
    output_dir.mkdir(parents=True, exist_ok=True)

    daily_dir = output_dir / "timeseries" / "daily"
    raw_dams_dir = output_dir / "raw" / "dams"
    raw_middles_dir = output_dir / "raw" / "rsvmiddles"
    metadata_path = output_dir / "metadata" / "thailand_reservoirs.csv"

    skip_existing = os.environ.get("SKIP_EXISTING_DAILY", "1") != "0"
    sleep_seconds = float(os.environ.get("THAILAND_SLEEP", "1.2"))
    # Raw JSON payloads are large (rsvmiddles ≈ 400 KB/day pretty-printed). Keep
    # them opt-in so the committed Actions repo doesn't grow indefinitely.
    save_raw = os.environ.get("SAVE_RAW_JSON", "0") != "0"

    session = requests.Session()
    dates = target_dates()
    print(f"[INFO] target dates: {dates[0]}..{dates[-1]} ({len(dates)} days)")

    wrote_any = False
    errors = 0
    fresh_rows: list[dict] = []

    for i, date_str in enumerate(dates):
        daily_path = daily_dir / f"thailand_timeseries_{date_str}.csv"
        if skip_existing and daily_path.exists():
            print(f"[SKIP] {daily_path.name}")
            continue

        print(f"[FETCH] {date_str}")
        large_rows: list[dict] = []
        middle_rows: list[dict] = []

        try:
            dams_json = post_json(session, DAMS_URL, _dams_payload(date_str))
            if save_raw:
                save_raw_json(raw_dams_dir / f"{date_str}.json", dams_json)
            large_rows = flatten_large(dams_json)
        except Exception as e:
            errors += 1
            print(f"[ERROR] dams {date_str}: {e}", file=sys.stderr)
            traceback.print_exc()

        try:
            middles_json = post_json(session, MIDDLES_URL, _middles_payload(date_str))
            if save_raw:
                save_raw_json(raw_middles_dir / f"{date_str}.json", middles_json)
            middle_rows = flatten_middle(middles_json)
        except Exception as e:
            errors += 1
            print(f"[ERROR] rsvmiddles {date_str}: {e}", file=sys.stderr)
            traceback.print_exc()

        combined = large_rows + middle_rows
        if combined:
            write_timeseries_csv(daily_path, combined)
            fresh_rows.extend(combined)
            wrote_any = True
            print(f"[OK] {daily_path.name}  ({len(large_rows)} large + {len(middle_rows)} middle)")

        if i + 1 < len(dates):
            time.sleep(sleep_seconds)

    count = upsert_metadata(
        metadata_path, fresh_rows,
        raw_dams_dir if save_raw else None,
        raw_middles_dir if save_raw else None,
    )
    print(f"[METADATA] {count} reservoirs -> {metadata_path.name}")

    if errors and not wrote_any:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

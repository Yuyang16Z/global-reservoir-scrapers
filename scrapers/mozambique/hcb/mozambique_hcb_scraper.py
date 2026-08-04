"""Mozambique HCB (Hidroelectrica de Cahora Bassa) hydrology API scraper.

Source:
- https://hcbadm.mzbusiness.com/wp-json/hydrology-post/v1/data
  Unauthenticated WordPress JSON endpoint backing the hcb.co.mz homepage
  widget for the Cahora Bassa reservoir.

Known quirks:
- The endpoint returns the WHOLE daily table on every call (records since
  2025-03-06), so a single successful run recovers all prior misses; dated
  raw snapshots are kept for provenance.
- Records are JSON objects with string values: cota (m), caudalafluente
  (m3/s), caudalefluente (m3/s), humidade (%), temperaturamaxima /
  temperaturaminima (C), precipitacao (mm), post_date, reference_date.
- Numeric strings may use comma OR dot decimals; normalise both.
- The same reference_date can appear in several records (corrections posted
  later); the record with the highest numeric id wins.
- The site updates mornings, Africa/Maputo = UTC+2.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
- raw/hydrology_<UTC stamp>.json               dated raw snapshot per run
- timeseries/mozambique_hcb_daily.csv          wide daily table merged by
                                               reference_date
- run_logs/runs.jsonl                          one JSON line per run
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")

RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

API_URL = "https://hcbadm.mzbusiness.com/wp-json/hydrology-post/v1/data"
TIMEOUT = 180
RETRIES = 3

DAILY_COLUMNS = [
    "reference_date",
    "water_level_m",
    "inflow_m3s",
    "outflow_m3s",
    "precipitation_mm",
    "humidity_pct",
    "temp_max_c",
    "temp_min_c",
    "post_date",
    "source_row_id",
    "fetched_at",
]

FIELD_MAP = {
    "water_level_m": "cota",
    "inflow_m3s": "caudalafluente",
    "outflow_m3s": "caudalefluente",
    "precipitation_mm": "precipitacao",
    "humidity_pct": "humidade",
    "temp_max_c": "temperaturamaxima",
    "temp_min_c": "temperaturaminima",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def append_run_log(payload: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (RUN_LOG_DIR / "runs.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def get_with_retries(url: str) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            return r
        except requests.RequestException as exc:
            last_exc = exc
            print(f"[retry] {url} attempt {attempt}/{RETRIES}: {exc!r}", flush=True)
            if attempt < RETRIES:
                time.sleep(10 * attempt)
    assert last_exc is not None
    raise last_exc


def clean_num(raw) -> str:
    """Normalise comma-or-dot decimal strings; return '' when not numeric."""
    if raw is None:
        return ""
    s = str(raw).strip().replace("\xa0", "").replace(" ", "")
    s = s.replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return m.group(0) if m else ""


def clean_date(raw) -> str:
    s = str(raw or "").strip()
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if not m:
        return ""
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def records_to_rows(payload, fetched_at: str) -> list[dict]:
    if isinstance(payload, dict):
        # tolerate a wrapper object: find the first list value
        payload = next((v for v in payload.values() if isinstance(v, list)), [])
    if not isinstance(payload, list):
        raise RuntimeError("API payload is neither a list nor a wrapped list")
    best: dict[str, dict] = {}
    best_id: dict[str, float] = {}
    for rec in payload:
        if not isinstance(rec, dict):
            continue
        ref = clean_date(rec.get("reference_date"))
        if not ref:
            continue
        try:
            rid = float(rec.get("id", 0) or 0)
        except (TypeError, ValueError):
            rid = 0.0
        # highest source row id wins for a repeated reference_date
        if ref in best_id and rid < best_id[ref]:
            continue
        row = {"reference_date": ref,
               "post_date": clean_date(rec.get("post_date")),
               "source_row_id": str(rec.get("id", "")).strip(),
               "fetched_at": fetched_at}
        for col, key in FIELD_MAP.items():
            row[col] = clean_num(rec.get(key))
        best[ref] = row
        best_id[ref] = rid
    return list(best.values())


def merge_csv(path: Path, columns: list[str], new_rows: list[dict],
              key_fields: list[str]) -> tuple[int, int]:
    existing: dict[tuple, dict] = {}
    if path.exists():
        with path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                key = tuple(str(row.get(k, "")) for k in key_fields)
                existing[key] = {c: str(row.get(c, "")) for c in columns}
    added = updated = 0
    for row in new_rows:
        norm = {c: str(row.get(c, "")) for c in columns}
        key = tuple(norm[k] for k in key_fields)
        if key not in existing:
            added += 1
        else:
            old = existing[key]
            # idempotence: ignore the fetched_at column when comparing
            if {c: old[c] for c in columns if c != "fetched_at"} == \
               {c: norm[c] for c in columns if c != "fetched_at"}:
                continue
            updated += 1
        existing[key] = norm
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for key in sorted(existing):
            w.writerow(existing[key])
    print(f"[SAVE] {path} total={len(existing)} added={added} updated={updated}",
          flush=True)
    return added, updated


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    log = {
        "started_at": fetched_at,
        "source": "mozambique/hcb",
        "url": API_URL,
        "output_dir": str(OUTPUT_DIR),
        "status": "started",
        "errors": [],
    }
    try:
        r = get_with_retries(API_URL)
        if r.status_code != 200:
            raise RuntimeError(f"API returned HTTP {r.status_code}")
        body = r.content
        if not body.lstrip().startswith((b"[", b"{")):
            raise RuntimeError("API response is not JSON")
        payload = json.loads(body)

        raw_path = RAW_DIR / f"hydrology_{utc_stamp()}.json"
        raw_path.write_bytes(body)
        print(f"[SAVE] {raw_path} ({len(body)}B)", flush=True)
        log["raw_file"] = raw_path.name

        rows = records_to_rows(payload, fetched_at)
        if not rows:
            raise RuntimeError("API payload parsed to zero daily records")
        added, updated = merge_csv(TS_DIR / "mozambique_hcb_daily.csv",
                                   DAILY_COLUMNS, rows, ["reference_date"])
        dates = sorted(row["reference_date"] for row in rows)
        log["status"] = "ok"
        log["records"] = len(rows)
        log["date_range"] = [dates[0], dates[-1]]
        log["rows_added"] = added
        log["rows_updated"] = updated
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        return 0
    except Exception as exc:
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

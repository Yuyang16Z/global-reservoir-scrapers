"""Ghana Bui Power Authority reservoir-level scraper.

Source:
- https://buipower.com/data/water-level-public.json   rolling daily level trends
- https://buipower.com/data/generation-public.json    companion generation file
- https://buipower.com/api/power_status.php           spot level + inflow/outflow

Known quirks:
- A browser User-Agent is REQUIRED: the site runs Mod_Security and answers
  bare clients with HTTP 406 "Not Acceptable".
- `power_status.php` returns 406 even with a browser User-Agent from server
  infrastructure (verified 2026-08-03). It is still probed every run because
  it is the only public route to Bui inflow/outflow; a 406 is treated as an
  expected miss, not a run failure.
- `water-level-public.json` is a ROLLING WINDOW of roughly one year, carried in
  three overlapping arrays (trend_ytd, trend_90d, trend_30d) plus a current
  spot value. Anything that scrolls out of the window is lost, so this scraper
  must run continuously; every array is merged into the same daily CSV.
- The file's `updated_at` can be newer than its last trend point, so the spot
  `current_level_m` is merged under `latest_actual_date`, not under
  `updated_at`.
- Levels are metres above sea level (operating range roughly 168-183 m).

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/water_level_<stamp>.json, raw/generation_<stamp>.json,
  raw/power_status_<stamp>.json (only when the endpoint answers)
  timeseries/ghana_bui_daily.csv   (merged, idempotent)
  timeseries/ghana_bui_spot.csv    (append-only spot inflow/outflow readings)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import json
import os
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

LEVEL_URL = "https://buipower.com/data/water-level-public.json"
GENERATION_URL = "https://buipower.com/data/generation-public.json"
STATUS_URL = "https://buipower.com/api/power_status.php"
SOURCE_PAGE = "https://buipower.com/"
RESERVOIR_ID = "GH_BPA_BUI"
TIMEOUT = 120
RETRIES = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": SOURCE_PAGE,
}

DAILY_COLUMNS = ["measurement_date", "water_level_masl", "reservoir_id",
                 "source_array", "fetched_at"]
SPOT_COLUMNS = ["reading_date", "water_level_masl", "inflow_m3s", "outflow_m3s",
                "reservoir_id", "fetched_at"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def get_with_retries(url: str, *, tolerate: tuple[int, ...] = ()):
    """Return the response, or None when the endpoint is unavailable.

    Status codes in `tolerate` return None immediately without retrying (used
    for the WAF-blocked status endpoint).
    """
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        except requests.RequestException as exc:
            print(f"[WARN] {url} attempt {attempt}/{RETRIES}: {exc!r}", flush=True)
            time.sleep(10 * attempt)
            continue
        if r.status_code in tolerate:
            print(f"[INFO] {url}: HTTP {r.status_code} (expected block)", flush=True)
            return None
        if r.status_code == 200:
            return r
        last = r.status_code
        print(f"[WARN] {url} attempt {attempt}/{RETRIES}: HTTP {r.status_code}",
              flush=True)
        time.sleep(10 * attempt)
    print(f"[WARN] {url}: giving up (last status {last})", flush=True)
    return None


def clean_num(value) -> str:
    if value is None:
        return ""
    s = str(value).strip().replace(",", ".")
    if not s:
        return ""
    try:
        return f"{float(s):g}"
    except ValueError:
        return ""


def clean_date(value) -> str:
    s = str(value or "").strip()[:10]
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return ""


def level_rows(payload: dict, fetched_at: str) -> list[dict]:
    """Merge the three overlapping trend arrays plus the current spot value."""
    best: dict[str, dict] = {}
    # widest window first so a narrower, fresher array can overwrite it
    for array_name in ("trend_ytd", "trend_90d", "trend_30d"):
        for rec in payload.get(array_name) or []:
            d = clean_date(rec.get("date"))
            v = clean_num(rec.get("level"))
            if d and v:
                best[d] = {"measurement_date": d, "water_level_masl": v,
                           "reservoir_id": RESERVOIR_ID,
                           "source_array": array_name, "fetched_at": fetched_at}
    d = clean_date(payload.get("latest_actual_date"))
    v = clean_num(payload.get("current_level_m"))
    if d and v:
        best[d] = {"measurement_date": d, "water_level_masl": v,
                   "reservoir_id": RESERVOIR_ID,
                   "source_array": "current_level_m", "fetched_at": fetched_at}
    return list(best.values())


def status_row(payload: dict, fetched_at: str) -> dict | None:
    level = payload.get("water_level_m")
    if isinstance(level, dict):
        level = level.get("now")
    d = clean_date(payload.get("latest_actual_date") or payload.get("updated_at"))
    row = {"reading_date": d,
           "water_level_masl": clean_num(level),
           "inflow_m3s": clean_num(payload.get("inflow_m3s")),
           "outflow_m3s": clean_num(payload.get("outflow_m3s")),
           "reservoir_id": RESERVOIR_ID,
           "fetched_at": fetched_at}
    if not d or not (row["inflow_m3s"] or row["outflow_m3s"] or
                     row["water_level_masl"]):
        return None
    return row


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
            compare = [c for c in columns if c not in ("fetched_at", "source_array")]
            if {c: old[c] for c in compare} == {c: norm[c] for c in compare}:
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


def append_run_log(log: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = RUN_LOG_DIR / f"{utc_stamp()}_summary.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {path}", flush=True)


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    stamp = utc_stamp()
    log = {"started_at": fetched_at, "source": "ghana/bui",
           "urls": [LEVEL_URL, GENERATION_URL, STATUS_URL],
           "output_dir": str(OUTPUT_DIR), "status": "started", "errors": []}
    try:
        # --- daily level trends (the series that matters)
        r = get_with_retries(LEVEL_URL)
        if r is None:
            raise RuntimeError("water-level-public.json unavailable after retries")
        raw_path = RAW_DIR / f"water_level_{stamp}.json"
        raw_path.write_bytes(r.content)
        print(f"[SAVE] {raw_path} ({len(r.content)}B)", flush=True)
        payload = json.loads(r.content)
        rows = level_rows(payload, fetched_at)
        if not rows:
            raise RuntimeError("level payload parsed to zero daily records")
        added, updated = merge_csv(TS_DIR / "ghana_bui_daily.csv",
                                   DAILY_COLUMNS, rows, ["measurement_date"])
        dates = sorted(row["measurement_date"] for row in rows)
        log.update({"level_records": len(rows), "date_range": [dates[0], dates[-1]],
                    "rows_added": added, "rows_updated": updated,
                    "raw_level_file": raw_path.name})

        # --- companion generation file (archived only)
        g = get_with_retries(GENERATION_URL)
        if g is not None:
            gen_path = RAW_DIR / f"generation_{stamp}.json"
            gen_path.write_bytes(g.content)
            print(f"[SAVE] {gen_path} ({len(g.content)}B)", flush=True)
            log["raw_generation_file"] = gen_path.name
        else:
            log["generation_status"] = "unavailable"

        # --- spot status (inflow/outflow); 406 is the documented normal case
        s = get_with_retries(STATUS_URL, tolerate=(406, 403))
        if s is not None and s.content.lstrip().startswith((b"{", b"[")):
            st_path = RAW_DIR / f"power_status_{stamp}.json"
            st_path.write_bytes(s.content)
            print(f"[SAVE] {st_path} ({len(s.content)}B)", flush=True)
            row = status_row(json.loads(s.content), fetched_at)
            if row:
                a2, u2 = merge_csv(TS_DIR / "ghana_bui_spot.csv", SPOT_COLUMNS,
                                   [row], ["reading_date"])
                log.update({"spot_added": a2, "spot_updated": u2})
            log["raw_status_file"] = st_path.name
        else:
            log["status_endpoint"] = "blocked_or_unavailable"

        log["status"] = "ok"
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        return 0
    except Exception as exc:  # noqa: BLE001 - scheduled job: log and signal
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

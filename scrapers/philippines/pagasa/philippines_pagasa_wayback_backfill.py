"""Backfill Philippines PAGASA dam snapshots from the Wayback Machine.

The live PAGASA flood page only publishes the last 2 days. To reconstruct
multi-year history we walk every daily-collapsed Wayback capture of
https://www.pagasa.dost.gov.ph/flood, re-use the parsing logic from the daily
scraper, and merge extracted observations into the same per-date snapshot CSVs.

Env vars:
    OUTPUT_DIR            Override output root (default: data/philippines/pagasa)
    PHILIPPINES_WB_FROM   Earliest snapshot date (YYYY-MM-DD, default: 2018-01-01)
    PHILIPPINES_WB_TO     Latest snapshot date (YYYY-MM-DD, default: today)
    PHILIPPINES_WB_LIMIT  Optional integer, stop after N snapshots (smoke test)
    PHILIPPINES_WB_DELAY  Seconds between Wayback fetches (default: 1.5)

Usage (local):
    python scrapers/philippines/pagasa/philippines_pagasa_wayback_backfill.py

Note:
    Wayback coverage of this page is sparse (~160 unique days 2021-09 to now).
    This script is idempotent — reruns add new dates without overwriting
    already-captured observations.
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))

from philippines_pagasa_scraper import (  # noqa: E402
    PH_TZ,
    SNAPSHOT_COLUMNS,
    extract_page_timestamp,
    find_dam_table,
    merge_snapshot,
    observation_to_snapshot_row,
    parse_dam_table,
    resolve_output_dir,
    write_csv,
)

CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_URL_FMT = "https://web.archive.org/web/{timestamp}id_/https://www.pagasa.dost.gov.ph/flood"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
TIMEOUT = 60


def cdx_query(session: requests.Session, from_date: str, to_date: str) -> list[dict[str, str]]:
    params = {
        "url": "pagasa.dost.gov.ph/flood",
        "from": from_date.replace("-", ""),
        "to": to_date.replace("-", ""),
        "output": "json",
        "filter": ["statuscode:200", "mimetype:text/html"],
        "collapse": "timestamp:8",
    }
    response = session.get(CDX_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    rows = response.json()
    if not rows:
        return []
    header, *records = rows
    return [dict(zip(header, r)) for r in records]


def fetch_snapshot(
    session: requests.Session, timestamp: str, cache_dir: Path
) -> str:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"flood_{timestamp}.html"
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")
    url = WAYBACK_URL_FMT.format(timestamp=timestamp)
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    html = response.text
    cache_path.write_text(html, encoding="utf-8")
    return html


def process_snapshot(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    _, page_date = extract_page_timestamp(soup)
    table = find_dam_table(soup)
    observations = parse_dam_table(table, page_date)
    return [observation_to_snapshot_row(obs) for obs in observations]


def main() -> int:
    output_dir = resolve_output_dir()
    daily_dir = output_dir / "timeseries" / "daily"
    run_log_dir = output_dir / "run_logs"
    cache_dir = output_dir / "raw" / "wayback_html"

    for directory in (daily_dir, run_log_dir):
        directory.mkdir(parents=True, exist_ok=True)

    from_date = os.environ.get("PHILIPPINES_WB_FROM", "2018-01-01")
    to_date = os.environ.get("PHILIPPINES_WB_TO", date.today().isoformat())
    limit_env = os.environ.get("PHILIPPINES_WB_LIMIT")
    limit = int(limit_env) if limit_env else None
    delay = float(os.environ.get("PHILIPPINES_WB_DELAY", "1.5"))

    run_stamp = datetime.now(PH_TZ).strftime("%Y%m%d_%H%M%S")
    summary_path = run_log_dir / f"{run_stamp}_backfill_summary.json"
    summary: dict[str, Any] = {
        "run_time": datetime.now(PH_TZ).isoformat(timespec="seconds"),
        "from_date": from_date,
        "to_date": to_date,
        "status": "started",
        "cdx_count": 0,
        "processed": 0,
        "observations_added": 0,
        "dates_touched": [],
        "errors": [],
    }

    try:
        print(f"[INFO] OUTPUT_DIR = {output_dir}")
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        records = cdx_query(session, from_date, to_date)
        if limit:
            records = records[:limit]
        summary["cdx_count"] = len(records)
        print(f"[CDX] {len(records)} daily-unique snapshots from {from_date} to {to_date}")

        snapshots_by_date: dict[str, list[dict[str, str]]] = {}
        for index, record in enumerate(records, start=1):
            ts = record["timestamp"]
            try:
                html = fetch_snapshot(session, ts, cache_dir)
                rows = process_snapshot(html)
                for row in rows:
                    snapshots_by_date.setdefault(row["date"], []).append(row)
                print(f"[{index}/{len(records)}] {ts} -> {len(rows)} rows")
            except Exception as exc:
                summary["errors"].append({"timestamp": ts, "error": str(exc)})
                print(f"[{index}/{len(records)}] {ts} FAILED: {exc}", file=sys.stderr)
            if delay and index < len(records):
                time.sleep(delay)

        dates_touched: list[str] = []
        for snapshot_date, rows in sorted(snapshots_by_date.items()):
            path = daily_dir / f"philippines_timeseries_{snapshot_date}.csv"
            merged = merge_snapshot(path, rows)
            write_csv(path, SNAPSHOT_COLUMNS, merged)
            dates_touched.append(snapshot_date)
            summary["observations_added"] += len(rows)

        summary.update(
            {
                "status": "success",
                "processed": len(records) - len(summary["errors"]),
                "dates_touched": dates_touched,
            }
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[DONE] snapshots={len(records)} dates={len(dates_touched)} "
            f"errors={len(summary['errors'])}"
        )
        return 0
    except Exception as exc:
        summary.update(
            {
                "status": "failed",
                "fatal_error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(traceback.format_exc(), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

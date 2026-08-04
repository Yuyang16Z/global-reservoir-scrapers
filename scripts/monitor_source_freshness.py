#!/usr/bin/env python3
"""Detect silently stale scraped sources.

WINDOWED_SOURCE_POLICY.md ("Monitoring and recovery") requires every deployment
to expose the latest archived observation date and to be treated as stale once
that date falls behind the expected publication cadence plus a grace period.
Nothing implemented that check, so a source could go dark while its workflow
stayed green - which is exactly what a scraper that tolerates outages will do.

For each registered source this script reads the newest run summary under
`data/<path>/run_logs/`, plus the newest observation date it can find in that
source's committed data, and compares both against
`publication_cadence_hours * STALE_CADENCE_FACTOR + max_schedule_gap_hours`.

Exit codes:
  0  every source fresh (or explicitly excused)
  1  at least one source stale        -> the scheduled workflow turns red
  2  registry or data layout problem

Usage:
  python3 scripts/monitor_source_freshness.py [--json report.json] [--quiet]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "config" / "windowed_sources.json"

# How many publication cycles a source may miss before it counts as stale.
STALE_CADENCE_FACTOR = 3.0
# Sources whose upstream is known-dormant: still reported, never fail the run.
# Keep the reason and the review date so these cannot rot unnoticed.
EXCUSED: dict[str, str] = {}

DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")


def parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    v = value.strip().replace("Z", "+00:00")
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.fromisoformat(v) if fmt is None else datetime.strptime(v, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def newest_run(data_dir: Path) -> dict:
    """Latest run summary plus the outcome mix of the recent runs."""
    log_dir = data_dir / "run_logs"
    logs = sorted(log_dir.glob("*.json")) if log_dir.is_dir() else []
    if not logs:
        return {}
    recent = logs[-10:]
    statuses = []
    for p in recent:
        try:
            statuses.append(json.loads(p.read_text(encoding="utf-8")).get("status", "?"))
        except (json.JSONDecodeError, OSError):
            statuses.append("unreadable")
    try:
        latest = json.loads(logs[-1].read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"recent_statuses": statuses}
    latest["recent_statuses"] = statuses
    latest["_log_file"] = logs[-1].name
    return latest


def newest_observation(data_dir: Path) -> str | None:
    """Newest ISO date visible in this source's committed data.

    Reads CSV date-ish columns where possible and falls back to dated
    filenames, so it works across the repo's differing table layouts.
    """
    best: str | None = None
    for csv_path in sorted(data_dir.rglob("*.csv")):
        if "run_logs" in csv_path.parts:
            continue
        try:
            with csv_path.open(encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                fields = reader.fieldnames or []
                date_cols = [c for c in fields
                             if re.search(r"date|day|time", c or "", re.I)
                             and not re.search(r"fetched|post_date|retrieved|updated",
                                               c or "", re.I)]
                if not date_cols:
                    continue
                for row in reader:
                    for c in date_cols:
                        m = DATE_RE.search(str(row.get(c) or ""))
                        if m and (best is None or m.group(1) > best):
                            best = m.group(1)
        except (OSError, csv.Error, UnicodeDecodeError):
            continue
    if best is None:
        for p in data_dir.rglob("*"):
            m = DATE_RE.search(p.name) or re.search(r"(20\d{2})(\d{2})(\d{2})", p.name)
            if not m:
                continue
            d = m.group(1) if "-" in m.group(0) else f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            if best is None or d > best:
                best = d
    return best


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", type=Path, help="write the full report here")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if not REGISTRY.is_file():
        print(f"[ERROR] registry not found: {REGISTRY}", file=sys.stderr)
        return 2
    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    now = datetime.now(timezone.utc)

    rows: list[dict] = []
    for source in registry.get("sources", []):
        sid = source["source_id"]
        if source.get("deployment_status") != "active":
            rows.append({"source_id": sid, "state": "not_active",
                         "deployment_status": source.get("deployment_status")})
            continue
        data_dir = ROOT / source["data_path"]
        if not data_dir.is_dir():
            rows.append({"source_id": sid, "state": "no_data_dir",
                         "data_path": source["data_path"]})
            continue

        cadence = float(source.get("publication_cadence_hours") or 24)
        grace = float(source.get("max_schedule_gap_hours") or cadence)
        budget_h = cadence * STALE_CADENCE_FACTOR + grace

        run = newest_run(data_dir)
        obs = newest_observation(data_dir)
        obs_dt = parse_iso(obs) if obs else None
        obs_age_h = (now - obs_dt).total_seconds() / 3600 if obs_dt else None
        run_dt = parse_iso(str(run.get("finished_at") or run.get("started_at") or ""))
        run_age_h = (now - run_dt).total_seconds() / 3600 if run_dt else None

        stale = obs_age_h is not None and obs_age_h > budget_h
        row = {
            "source_id": sid,
            "state": "stale" if stale else "fresh",
            "latest_observation": obs,
            "observation_age_hours": round(obs_age_h, 1) if obs_age_h is not None else None,
            "stale_after_hours": round(budget_h, 1),
            "latest_run_status": run.get("status"),
            "latest_run_age_hours": round(run_age_h, 1) if run_age_h is not None else None,
            "recent_run_statuses": run.get("recent_statuses"),
        }
        if obs_age_h is None:
            row["state"] = "no_observation_date"
        if sid in EXCUSED:
            row["excused"] = EXCUSED[sid]
        rows.append(row)

    problems = [r for r in rows
                if r["state"] in {"stale", "no_observation_date", "no_data_dir"}
                and "excused" not in r]

    if not args.quiet:
        width = max((len(r["source_id"]) for r in rows), default=10)
        for r in sorted(rows, key=lambda x: (x["state"] != "stale", x["source_id"])):
            age = r.get("observation_age_hours")
            age_s = f"{age/24:.1f}d" if age is not None else "-"
            budget = r.get("stale_after_hours")
            budget_s = f"{budget/24:.1f}d" if budget is not None else "-"
            flag = "STALE" if r["state"] == "stale" else r["state"]
            note = "  (excused)" if "excused" in r else ""
            print(f"{r['source_id']:<{width}}  {flag:<20} "
                  f"latest={r.get('latest_observation') or '-':<12} "
                  f"age={age_s:>7} / budget={budget_s:>7} "
                  f"run={r.get('latest_run_status') or '-'}{note}")
        print()
        print(f"{len(rows)} registered source(s); {len(problems)} need attention")

    report = {
        "generated_at": now.isoformat(timespec="seconds"),
        "stale_cadence_factor": STALE_CADENCE_FACTOR,
        "sources": rows,
        "problems": [r["source_id"] for r in problems],
        "status": "PASS" if not problems else "FAIL",
    }
    if args.json:
        args.json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                             encoding="utf-8")
        print(f"[SAVE] {args.json}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

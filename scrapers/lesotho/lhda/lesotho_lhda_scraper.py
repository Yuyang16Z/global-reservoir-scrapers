"""Lesotho LHDA dam fill-rate scraper.

Source:
- https://www.lhda.org.ls/  homepage widget, Lesotho Highlands Development
  Authority, which operates Katse and Mohale.

Why this source exists in the repo:
- South Africa's DWS weekly bulletin used to carry Katse and Mohale (they feed
  the Lesotho Highlands Water Project into South Africa) and the project's
  Lesotho delivery was built from it. DWS **dropped the three foreign dams from
  its bulletin on 2026-06-16**, freezing that delivery. LHDA is the operator of
  these dams and publishes their fill percentage itself, so it is the natural
  and arguably more authoritative continuation.

Known quirks:
- The widget is a CURRENT-VALUE SNAPSHOT: it shows one figure per dam and is
  overwritten, so anything not captured is lost. Hence the frequent schedule.
- The observation date is the widget's own printed "updated on: DD-MM-YYYY",
  never the fetch date, and each dam carries its own date.
- Only a percentage is published. No volume, and no capacity to derive one
  from, so `storage_mcm` is not available from this source.
- The percentage can exceed 100 (Mohale read 100.16% on 2026-08-30); that is
  the operator's own figure and is recorded as printed.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/lhda_home_<stamp>.html
  metadata/lesotho_lhda_reservoirs.csv
  timeseries/lesotho_lhda_timeseries.csv   (merged, idempotent)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import html as html_mod
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
META_DIR = OUTPUT_DIR / "metadata"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

HOME_URL = "https://www.lhda.org.ls/"
SOURCE_AGENCY = "LHDA (Lesotho Highlands Development Authority)"
TIMEOUT = 90
REQUEST_ATTEMPTS = 3
REQUEST_BACKOFFS = (2, 8, 20)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/146.0.0.0 Safari/537.36")
}

# The dams this source is trusted for. The same widget also shows construction
# progress percentages for tunnels and bridges under the Phase II works; those
# are project milestones, not reservoir storage, and must never be read as data.
DAMS = {"KATSE DAM": "Katse", "MOHALE DAM": "Mohale", "POLIHALI DAM": "Polihali"}

# <h2>Katse Dam</h2> ... <li>80.94% of the capacity of the dam</li>
#                        <li><small>updated on: 30-08-2026</small></li>
# The page wraps and indents its markup, so the sentence is routinely split
# across lines ("80.94% of the\n    capacity of the dam"). Every literal gap
# below therefore has to tolerate arbitrary whitespace.
BLOCK_RE = re.compile(
    r"<h2[^>]*>\s*([^<]+?)\s*</h2>(.{0,600}?)(?=<h2|\Z)", re.S | re.I)
PCT_RE = re.compile(r"([\d.]+)\s*%\s*of\s+the\s+capacity", re.I | re.S)
DATE_RE = re.compile(r"updated\s+on:\s*(\d{2})-(\d{2})-(\d{4})", re.I | re.S)

TS_COLUMNS = ["measurement_date", "reservoir_id", "reservoir_name", "storage_pct",
              "fetched_at"]


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def fetch(url: str) -> requests.Response | None:
    last: Exception | None = None
    for attempt, backoff in enumerate(REQUEST_BACKOFFS, 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            last = RuntimeError(f"HTTP {r.status_code}")
        except requests.RequestException as exc:  # noqa: PERF203
            last = exc
        if attempt < REQUEST_ATTEMPTS:
            log(f"  attempt {attempt} failed ({last!r}); retrying in {backoff}s")
            time.sleep(backoff)
    log(f"  giving up on {url}: {last!r}")
    return None


def parse(page: str) -> list[dict]:
    """Every dam block the widget shows, with the widget's own printed date."""
    out: list[dict] = []
    text = html_mod.unescape(page)
    for name, body in BLOCK_RE.findall(text):
        key = re.sub(r"\s+", " ", name).strip().upper()
        canonical = DAMS.get(key)
        if not canonical:
            continue
        pct = PCT_RE.search(body)
        date = DATE_RE.search(body)
        if not pct or not date:
            log(f"  [skip] {canonical}: block found but "
                f"{'percentage' if not pct else 'date'} missing")
            continue
        d, m, y = date.groups()
        out.append({
            "measurement_date": f"{y}-{m}-{d}",
            "reservoir_id": f"LS_LHDA_{canonical.upper()}",
            "reservoir_name": canonical,
            "storage_pct": pct.group(1),
        })
    return out


def merge(path: Path, rows: list[dict], stamp: str) -> int:
    """Idempotent merge keyed on (date, reservoir); existing values are kept."""
    existing: dict[tuple[str, str], dict] = {}
    if path.is_file():
        with path.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                existing[(r["measurement_date"], r["reservoir_id"])] = r
    added = 0
    for r in rows:
        key = (r["measurement_date"], r["reservoir_id"])
        if key in existing:
            continue
        existing[key] = {**r, "fetched_at": stamp}
        added += 1
    TS_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=TS_COLUMNS, extrasaction="ignore",
                           lineterminator="\n")
        w.writeheader()
        for key in sorted(existing):
            w.writerow(existing[key])
    return added


def main() -> int:
    stamp = utc_stamp()
    for d in (RAW_DIR, TS_DIR, META_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {"source": "lesotho/lhda", "started_at": stamp,
                                  "url": HOME_URL}

    resp = fetch(HOME_URL)
    if resp is None:
        # An unreachable host is a source outage, not a scraper failure.
        summary.update(status="source_unavailable", finished_at=utc_stamp())
        (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        log("[warn] lhda.org.ls unreachable; recorded source_unavailable, exiting 0")
        return 0

    (RAW_DIR / f"lhda_home_{stamp}.html").write_text(resp.text, encoding="utf-8")
    rows = parse(resp.text)
    if not rows:
        # The widget is the whole source; if its shape changed, say so loudly
        # rather than quietly recording a successful run with no data.
        summary.update(status="no_rows_parsed", finished_at=utc_stamp())
        (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        log("[error] page fetched but no dam block parsed; the widget markup "
            "may have changed", )
        return 1

    added = merge(TS_DIR / "lesotho_lhda_timeseries.csv", rows, stamp)

    with (META_DIR / "lesotho_lhda_reservoirs.csv").open(
            "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["reservoir_id", "reservoir_name", "country", "operator",
                    "source_agency", "source_url", "data_type", "last_updated"])
        for r in sorted(rows, key=lambda x: x["reservoir_id"]):
            w.writerow([r["reservoir_id"], r["reservoir_name"], "Lesotho",
                        SOURCE_AGENCY, SOURCE_AGENCY, HOME_URL, "in_situ", stamp])

    summary.update(status="ok", rows_seen=len(rows), rows_added=added,
                   observations=[{k: r[k] for k in
                                  ("measurement_date", "reservoir_name", "storage_pct")}
                                 for r in rows],
                   finished_at=utc_stamp())
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    log(f"[done] {len(rows)} dam(s) read, {added} new row(s): " +
        "; ".join(f"{r['reservoir_name']} {r['storage_pct']}% "
                  f"({r['measurement_date']})" for r in rows))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 - the run log must record the crash
        RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
        (RUN_LOG_DIR / f"{utc_stamp()}_crash.json").write_text(
            json.dumps({"source": "lesotho/lhda", "status": "crashed",
                        "traceback": traceback.format_exc()}, indent=2),
            encoding="utf-8")
        raise

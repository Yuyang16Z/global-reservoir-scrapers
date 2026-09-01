"""Zambezi River Authority - Lake Kariba levels and reservoir data.

Sources (both on https://www.zambezira.org/):
- /hydrology/lake-levels          daily lake level + % full, ~14-day window
- /hydrology/kariba-reservoir-data  one month, three years side by side, with
                                    live storage, turbine discharge, spillage
                                    and total outflow for the current year

Why this source exists in the repo:
- Kariba is one of the world's largest reservoirs by volume and is shared
  between Zambia and Zimbabwe. The project already delivers it, but had no
  automation: the delivery had been rebuilt by hand from whatever the monthly
  page happened to show, which is why 2022 and 2023 were missing entirely.

Retention classes, and why they differ:
- `lake-levels` is a **rolling window**: it shows roughly a fortnight and drops
  the oldest day as a new one arrives, so a day not captured within two weeks
  is lost. This is what the frequent schedule is for.
- `kariba-reservoir-data` shows the current month and is replaced when the
  month turns. Older months remain reachable as numbered pages, so history is
  recoverable, but capturing monthly avoids depending on that.

Known quirks:
- The daily table prints "18-Aug" with no year. The year comes from the page's
  own "From:"/"To:" fields, which carry full ISO dates in a `content=` attribute.
- A fortnight can span a year boundary, so the year is assigned per row by
  walking the range rather than applied wholesale.
- Percentages are "% usable storage" against the 475.50-488.50 m operating
  range, not against total capacity; they are recorded as the source states.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/lake_levels_<stamp>.html, reservoir_data_<stamp>.html
  timeseries/zambia_zra_kariba_daily.csv      (merged, idempotent)
  timeseries/zambia_zra_kariba_monthly.csv    (merged, idempotent)
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
from datetime import date, datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

LEVELS_URL = "https://www.zambezira.org/hydrology/lake-levels"
MONTHLY_URL = "https://www.zambezira.org/hydrology/kariba-reservoir-data"
SOURCE_AGENCY = "ZRA (Zambezi River Authority)"
RESERVOIR_ID = "ZM_ZRA_KARIBA"
TIMEOUT = 90
REQUEST_ATTEMPTS = 3
REQUEST_BACKOFFS = (3, 12, 30)

HEADERS = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/146.0.0.0 Safari/537.36")}

MONTHS = ("January February March April May June July August September "
          "October November December").split()
MONTH_ABBR = {m[:3].lower(): i + 1 for i, m in enumerate(MONTHS)}

RANGE_RE = re.compile(r'content="(\d{4})-(\d{2})-(\d{2})T', re.I)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
DAYMON_RE = re.compile(r"^\s*(\d{1,2})\s*-\s*([A-Za-z]{3})", re.I)

DAILY_COLUMNS = ["measurement_date", "reservoir_id", "water_level_masl",
                 "usable_storage_pct", "fetched_at"]
MONTHLY_COLUMNS = ["measurement_date", "reservoir_id", "water_level_masl",
                   "usable_storage_pct", "live_storage_bcm",
                   "turbine_discharge_m3s", "spillage_m3s", "total_outflow_m3s",
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


def cells(row_html: str) -> list[str]:
    out = []
    for c in CELL_RE.findall(row_html):
        text = html_mod.unescape(re.sub(r"<[^>]+>", " ", c))
        out.append(re.sub(r"\s+", " ", text).strip())
    return out


def number(text: str) -> str:
    t = (text or "").replace(",", "").replace("%", "").strip()
    return t if re.fullmatch(r"-?\d+(?:\.\d+)?", t) else ""


def parse_daily(page: str) -> list[dict]:
    """Daily rows from the fortnightly lake-levels table.

    The table prints "18-Aug" with no year, so the year is taken from the
    page's own From:/To: ISO dates and assigned by walking the range - a
    fortnight can cross a year boundary.
    """
    bounds = RANGE_RE.findall(page)
    if len(bounds) < 2:
        log("  [warn] lake-levels: no From/To dates found; daily rows skipped")
        return []
    start = date(*(int(x) for x in bounds[0]))
    end = date(*(int(x) for x in bounds[1]))

    rows: list[dict] = []
    for row_html in ROW_RE.findall(page):
        c = cells(row_html)
        if len(c) < 3:
            continue
        m = DAYMON_RE.match(c[0])
        if not m:
            continue
        day, mon = int(m.group(1)), MONTH_ABBR.get(m.group(2).lower())
        if not mon:
            continue
        # pick the year in [start, end] that this day-month belongs to
        chosen = None
        for year in {start.year, end.year}:
            try:
                cand = date(year, mon, day)
            except ValueError:
                continue
            if start <= cand <= end:
                chosen = cand
                break
        if chosen is None:
            continue
        level, pct = number(c[1]), number(c[2])
        if not level:
            continue
        rows.append({"measurement_date": chosen.isoformat(),
                     "reservoir_id": RESERVOIR_ID,
                     "water_level_masl": level, "usable_storage_pct": pct})
    return rows


def parse_monthly(page: str) -> list[dict]:
    """Current-year rows from the monthly three-year table.

    Only the current year's block carries storage and outflow; the earlier
    years repeat level and percentage only, and those dates are covered by
    their own captures, so they are not re-emitted here.
    """
    header_rows = [cells(r) for r in ROW_RE.findall(page)]
    header = next((h for h in header_rows if any(
        mm in " ".join(h) for mm in MONTHS)), None)
    if not header:
        log("  [warn] reservoir-data: month/year header not found")
        return []
    joined = " ".join(header)
    months_seen = [(m, i) for i, cell in enumerate(header) for m in MONTHS
                   if m in cell]
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", joined)]
    if not months_seen or not years:
        log("  [warn] reservoir-data: could not read month/year from header")
        return []
    month_num = MONTHS.index(months_seen[0][0]) + 1
    year = max(years)

    body = [c for c in header_rows if c and re.fullmatch(r"\d{1,2}", c[0] or "")]
    if not body:
        return []
    width = max(len(c) for c in body)
    # Column positions are derived, not assumed: the earlier years each occupy
    # a level and a percentage, so whatever follows day + 2*(years-1) cells is
    # the current year's block. Assuming a fixed width silently produced zero
    # rows when the table carried 11 columns rather than the 12 guessed at.
    leading = 1 + 2 * max(0, len(set(years)) - 1)
    tail = width - leading
    # current-year layout: level, live storage, % full, turbine, spillage, total
    CURRENT = ("water_level_masl", "live_storage_bcm", "usable_storage_pct",
               "turbine_discharge_m3s", "spillage_m3s", "total_outflow_m3s")
    rows: list[dict] = []
    for c in body:
        if len(c) < width:
            continue
        try:
            when = date(year, month_num, int(c[0]))
        except ValueError:
            continue
        rec = {"measurement_date": when.isoformat(), "reservoir_id": RESERVOIR_ID}
        block = c[leading:]
        if tail >= 6:
            for key, val in zip(CURRENT, block):
                rec[key] = number(val)
        elif tail >= 2:
            # an older layout that prints only level and percentage
            rec["water_level_masl"] = number(block[0])
            rec["usable_storage_pct"] = number(block[1])
        if any(rec.get(k) for k in MONTHLY_COLUMNS if k not in
               ("measurement_date", "reservoir_id", "fetched_at")):
            rows.append(rec)
    return rows


def merge(path: Path, columns: list[str], rows: list[dict], stamp: str) -> int:
    """Idempotent merge on measurement_date; delivered values are never changed."""
    existing: dict[str, dict] = {}
    if path.is_file():
        with path.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                existing[r["measurement_date"]] = r
    added = 0
    for r in rows:
        if r["measurement_date"] in existing:
            continue
        existing[r["measurement_date"]] = {**{c: "" for c in columns}, **r,
                                           "fetched_at": stamp}
        added += 1
    TS_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore",
                           lineterminator="\n")
        w.writeheader()
        for k in sorted(existing):
            w.writerow(existing[k])
    return added


def main() -> int:
    stamp = utc_stamp()
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {"source": "zambia/zra_kariba",
                                  "started_at": stamp}

    levels = fetch(LEVELS_URL)
    monthly = fetch(MONTHLY_URL)
    if levels is None and monthly is None:
        summary.update(status="source_unavailable", finished_at=utc_stamp())
        (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        log("[warn] zambezira.org unreachable; recorded source_unavailable, exit 0")
        return 0

    daily_added = monthly_added = 0
    if levels is not None:
        (RAW_DIR / f"lake_levels_{stamp}.html").write_text(levels.text,
                                                           encoding="utf-8")
        rows = parse_daily(levels.text)
        summary["daily_rows_seen"] = len(rows)
        daily_added = merge(TS_DIR / "zambia_zra_kariba_daily.csv",
                            DAILY_COLUMNS, rows, stamp)
    if monthly is not None:
        (RAW_DIR / f"reservoir_data_{stamp}.html").write_text(monthly.text,
                                                              encoding="utf-8")
        rows = parse_monthly(monthly.text)
        summary["monthly_rows_seen"] = len(rows)
        monthly_added = merge(TS_DIR / "zambia_zra_kariba_monthly.csv",
                              MONTHLY_COLUMNS, rows, stamp)

    summary.update(status="ok", daily_added=daily_added,
                   monthly_added=monthly_added, finished_at=utc_stamp())
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    log(f"[done] daily +{daily_added} (seen {summary.get('daily_rows_seen', 0)}), "
        f"monthly +{monthly_added} (seen {summary.get('monthly_rows_seen', 0)})")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 - the run log must record the crash
        RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
        (RUN_LOG_DIR / f"{utc_stamp()}_crash.json").write_text(
            json.dumps({"source": "zambia/zra_kariba", "status": "crashed",
                        "traceback": traceback.format_exc()}, indent=2),
            encoding="utf-8")
        raise

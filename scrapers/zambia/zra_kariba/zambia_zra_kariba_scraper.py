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
  month turns. Numbered pages kept older months only up to May 2021 (the newest
  the project's 2026-09 crawl of the site found), so a later day's storage and
  outflows survive only in captures taken while its month was on the page.

Late values:
- ZRA prints a day's turbine discharge, spillage and total outflow (now and
  then its live storage too) a day or more after the day's level, so the first
  capture of a day often lacks them: 2026-09-07 was captured with level and
  storage only and its outflows (1,229 m3/s) were on the page by 2026-09-17.
  Every run therefore replays all raw captures, oldest first, and a cell that
  is still blank takes the value of the first later capture that prints it.
  A value already held is never changed or blanked; a capture that prints it
  differently is listed in zra_kariba_revisions.csv instead (ZRA restates
  days). A difference of printed precision only (39 against 38.64) is not a
  revision. Replaying also means a parser fix recovers what an earlier
  capture held but the old parser missed.

Known quirks:
- The daily table prints "18-Aug" with no year. The year comes from the page's
  own "From:"/"To:" fields, which carry full ISO dates in a `content=` attribute.
- A fortnight can span a year boundary, so the year is assigned per row by
  walking the range rather than applied wholesale.
- Percentages are "% usable storage" against the 475.50-488.50 m operating
  range, not against total capacity; they are recorded as the source states.
- The monthly table names its year columns "September 2026" or, as on
  2026-09-24, "Sep-26"; that capture parsed to nothing until both were read.
- October's table prints its 31st row with an empty day cell. A row with an
  empty day cell straight after day 30 of a 31-day month is read as day 31:
  in the 2026 October page that row's 2024 cells (476.14 m, 4.46%) are the
  values a 2025-11-01 capture printed under a "31" label.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/lake_levels_<stamp>.html, reservoir_data_<stamp>.html
  timeseries/zambia_zra_kariba_daily.csv      (merged, idempotent)
  timeseries/zambia_zra_kariba_monthly.csv    (merged, idempotent)
  zra_kariba_revisions.csv                    (only once a revision is seen)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import calendar
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
# Outside timeseries/ on purpose: readers glob zambia_zra_kariba_*.csv there.
REVISIONS_PATH = OUTPUT_DIR / "zra_kariba_revisions.csv"

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
# Year columns of the monthly table: "September 2026", or "Sep-26" as printed
# on 2026-09-24.
MONTH_YEAR_RE = re.compile(r"\b(" + "|".join(MONTHS) + r")\s+(20\d{2})\b", re.I)
ABBR_YEAR_RE = re.compile(r"^\s*([A-Za-z]{3})[A-Za-z]*\s*[-' ]\s*(\d{2}|20\d{2})\s*$")
REVISION_COLUMNS = ["table", "measurement_date", "column", "kept_value",
                    "printed_value", "capture"]

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


def header_months(row: list[str]) -> list[tuple[int, int]]:
    """(month, year) of each year column a header row names, else []."""
    found = []
    for cell in row:
        m = MONTH_YEAR_RE.search(cell)
        if m:
            found.append((MONTH_ABBR[m.group(1)[:3].lower()], int(m.group(2))))
            continue
        m = ABBR_YEAR_RE.match(cell)
        if m and m.group(1).lower() in MONTH_ABBR:
            y = int(m.group(2))
            found.append((MONTH_ABBR[m.group(1).lower()], y + 2000 if y < 100 else y))
    return found


def parse_monthly(page: str) -> list[dict]:
    """Current-year rows from the monthly three-year table.

    Only the current year's block carries storage and outflow; the earlier
    years repeat level and percentage only, and those dates are covered by
    their own captures, so they are not re-emitted here.
    """
    header_rows = [cells(r) for r in ROW_RE.findall(page)]
    named = next((found for found in map(header_months, header_rows) if found), [])
    if not named:
        log("  [warn] reservoir-data: month/year header not found")
        return []
    months = {m for m, _ in named}
    years = {y for _, y in named}
    if len(months) != 1:
        log(f"  [warn] reservoir-data: header names several months {sorted(months)}")
        return []
    month_num = months.pop()
    year = max(years)

    # October's table leaves its 31st day cell empty; that row is the one
    # straight after day 30 (see the module docstring for the evidence).
    body: list[tuple[int, list[str]]] = []
    last_day = 0
    for c in header_rows:
        if not c:
            continue
        if re.fullmatch(r"\d{1,2}", c[0] or ""):
            last_day = int(c[0])
            body.append((last_day, c))
        elif (not c[0] and last_day == 30 and any(c[1:])
              and calendar.monthrange(year, month_num)[1] == 31):
            last_day = 31
            body.append((31, c))
    if not body:
        return []
    width = max(len(c) for _, c in body)
    # Column positions are derived, not assumed: the earlier years each occupy
    # a level and a percentage, so whatever follows day + 2*(years-1) cells is
    # the current year's block. Assuming a fixed width silently produced zero
    # rows when the table carried 11 columns rather than the 12 guessed at.
    leading = 1 + 2 * max(0, len(years) - 1)
    tail = width - leading
    # current-year layout: level, live storage, % full, turbine, spillage, total
    CURRENT = ("water_level_masl", "live_storage_bcm", "usable_storage_pct",
               "turbine_discharge_m3s", "spillage_m3s", "total_outflow_m3s")
    rows: list[dict] = []
    for day, c in body:
        if len(c) < width:
            continue
        try:
            when = date(year, month_num, day)
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


def decimals(text: str) -> int:
    return len(text.split(".")[1]) if "." in text else 0


def same_reading(held: str, printed: str) -> bool:
    """True when two printings are one reading, perhaps at different precision.

    The daily table printed 2026-10-02 as 38.64 on 2026-10-05 and as 39 on
    2026-10-15: a coarser print of the same number, not a restatement.
    """
    try:
        a, b = float(held), float(printed)
    except ValueError:
        return held == printed
    return abs(a - b) <= 0.5 * 10 ** -min(decimals(held), decimals(printed)) + 1e-9


def apply_capture(table: str, held: dict[str, dict], columns: list[str],
                  rows: list[dict], stamp: str, filled: list[dict],
                  revisions: list[dict]) -> int:
    """Merge one capture's rows into `held`; return the number of dates added.

    A new date is added with this capture's stamp. For a date already held, a
    blank cell takes this capture's value (appended to `filled`); a value
    already held is never changed or blanked, and a capture printing a
    different reading of it is appended to `revisions`.
    """
    keys = [c for c in columns if c not in ("measurement_date", "reservoir_id",
                                            "fetched_at")]
    added = 0
    for r in rows:
        d = r["measurement_date"]
        cur = held.get(d)
        if cur is None:
            held[d] = {**{c: "" for c in columns}, **r, "fetched_at": stamp}
            added += 1
            continue
        for c in keys:
            new, old = (r.get(c) or "").strip(), (cur.get(c) or "").strip()
            if not new:
                continue
            if not old:
                cur[c] = new
                filled.append({"table": table, "measurement_date": d,
                               "column": c, "value": new, "capture": stamp})
            elif not same_reading(old, new):
                revisions.append({"table": table, "measurement_date": d,
                                  "column": c, "kept_value": old,
                                  "printed_value": new, "capture": stamp})
    return added


def replay(table: str, prefix: str, parser, path: Path, columns: list[str],
           filled: list[dict], revisions: list[dict]) -> tuple[int, int]:
    """Re-apply every raw capture of one table, oldest first, to its CSV.

    Returns (dates added, captures read). Rows already in the CSV keep their
    values and fetched_at; only blank cells and new dates can change, so a
    rerun over the same captures changes nothing.
    """
    held: dict[str, dict] = {}
    if path.is_file():
        with path.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                held[r["measurement_date"]] = r
    added = captures = 0
    for raw in sorted(RAW_DIR.glob(f"{prefix}*.html")):
        stamp = raw.stem[len(prefix):]
        rows = parser(raw.read_text(encoding="utf-8"))
        added += apply_capture(table, held, columns, rows, stamp, filled, revisions)
        captures += 1
    TS_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore",
                           lineterminator="\n")
        w.writeheader()
        for k in sorted(held):
            w.writerow(held[k])
    return added, captures


def write_revisions(revisions: list[dict]) -> int:
    """Rewrite the revision list; each printed value once, at its first capture.

    Rebuilt from the replay on every run, so it needs no merging. Written only
    once there is something to list.
    """
    first: dict[tuple, dict] = {}
    for r in revisions:
        key = (r["table"], r["measurement_date"], r["column"], r["printed_value"])
        if key not in first or r["capture"] < first[key]["capture"]:
            first[key] = r
    if not first and not REVISIONS_PATH.is_file():
        return 0
    with REVISIONS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=REVISION_COLUMNS, lineterminator="\n")
        w.writeheader()
        for key in sorted(first, key=lambda k: (k[1], k[0], k[2], first[k]["capture"])):
            w.writerow(first[key])
    return len(first)


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

    if levels is not None:
        (RAW_DIR / f"lake_levels_{stamp}.html").write_text(levels.text,
                                                           encoding="utf-8")
        summary["daily_rows_seen"] = len(parse_daily(levels.text))
    if monthly is not None:
        (RAW_DIR / f"reservoir_data_{stamp}.html").write_text(monthly.text,
                                                              encoding="utf-8")
        summary["monthly_rows_seen"] = len(parse_monthly(monthly.text))

    # All captures so far, oldest first, this one included: a later capture
    # fills the cells an earlier one lacked (see "Late values" above).
    filled: list[dict] = []
    revisions: list[dict] = []
    daily_added, daily_read = replay(
        "daily", "lake_levels_", parse_daily,
        TS_DIR / "zambia_zra_kariba_daily.csv", DAILY_COLUMNS, filled, revisions)
    monthly_added, monthly_read = replay(
        "monthly", "reservoir_data_", parse_monthly,
        TS_DIR / "zambia_zra_kariba_monthly.csv", MONTHLY_COLUMNS, filled, revisions)
    revisions_listed = write_revisions(revisions)

    summary.update(status="ok", daily_added=daily_added,
                   monthly_added=monthly_added,
                   daily_filled=sum(f["table"] == "daily" for f in filled),
                   monthly_filled=sum(f["table"] == "monthly" for f in filled),
                   filled=filled, revisions_listed=revisions_listed,
                   captures_replayed=daily_read + monthly_read,
                   finished_at=utc_stamp())
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    log(f"[done] daily +{daily_added} (seen {summary.get('daily_rows_seen', 0)}, "
        f"{summary['daily_filled']} cells filled), monthly +{monthly_added} "
        f"(seen {summary.get('monthly_rows_seen', 0)}, "
        f"{summary['monthly_filled']} cells filled), "
        f"{revisions_listed} revision(s) listed")
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

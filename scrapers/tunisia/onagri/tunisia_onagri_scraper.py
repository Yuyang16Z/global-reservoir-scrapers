"""Tunisia ONAGRI/DGBGTH daily barrage situation bulletin scraper.

Source:
- https://www.onagri.nat.tn/Situation-des-barrages/fr/19   (archive listing)
- https://www.onagri.nat.tn/uploads/barrages/BARRAGES-{D-M-YYYY}.pdf  (2023-03+)
- https://www.onagri.nat.tn/uploads/barrages/{D-M-YYYY}.pdf           (to 2023-02)

Known quirks:
- PUBLICATION IS DORMANT: the last bulletin is 2025-10-10 and every later date
  404s at both URL patterns (re-verified 2026-08-03). This scraper probes the
  recent window every run as a cheap resumption watch; finding nothing is a
  successful run, not an error.
- Day and month are NOT zero-padded in either pattern, and both patterns must
  be tried for any date near the changeover.
- The server serves roughly 8-13 KB/s per connection, so timeouts are long.
- Page roles vary: the classic bulletin is (apports/lachers, hydraulic
  situation, Arabic summary), but the 2020-2022 Excel-exported issues reorder
  the pages. Pages are therefore identified by their printed headings.
- Column trap: from 2023 the apports page inserted a fill-percentage column,
  shifting the stock column right. 16-column era: col 13 = stock. 17-column
  era: col 13 = percentage, col 14 = stock. Getting this wrong silently
  swaps percentages into storage volumes.
- Date semantics: each bulletin prints "du <D-1> observee le <D>". Level,
  storage, withdrawal, rainfall and salinity are the 07:00 state of D, while
  inflow and outflow "du jour" are the day-volumes of D-1. Both are stored
  under their own observation dates.
- Inflow/outflow/withdrawal are VOLUMES per day (Mm3), not discharges.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/<year>/<bulletin>.pdf
  timeseries/tunisia_onagri_observations.csv   (long format, merged)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import traceback
from datetime import date as date_cls, datetime, timedelta, timezone
from pathlib import Path

import pdfplumber
import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

UPLOADS = "https://www.onagri.nat.tn/uploads/barrages"
LISTING = "https://www.onagri.nat.tn/Situation-des-barrages/fr/19"
TIMEOUT = 600
RETRIES = 3
PROBE_DAYS = int(os.environ.get("ONAGRI_PROBE_DAYS", "8"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

OBS_COLUMNS = ["obs_date", "reservoir_id", "variable", "value", "bulletin_file",
               "fetched_at"]
NUM_RE = re.compile(r"^-?\d{1,3}(?:[  ]?\d{3})*(?:,\d+)?$|^-?\d+(?:,\d+)?$")
NAME_YEAR_RE = re.compile(r"^(.*?)\s*\((\d{2,4})(?:\)|\s*\()")
AGG_TOKENS = ("TOTAL", "NORD", "CENTRE", "CAP-BON", "CAP BON", "DONT",
              "BARRAGES", "MOYENNE", "ENSEMBLE")
# page-2 (hydraulic situation) fixed column map, 26-column layout
P2 = {"cote_m": 18, "volume_mcm": 19, "volume_utilisable_mcm": 20,
      "soutirage_day_mcm": 21, "pluie_mm": 24, "salinite_gl": 25}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def get_pdf(url: str):
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        except requests.RequestException as exc:
            print(f"[WARN] {url.rsplit('/', 1)[-1]} attempt {attempt}/{RETRIES}: "
                  f"{exc!r}", flush=True)
            time.sleep(10 * attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 200 and r.content.startswith(b"%PDF"):
            return r
        print(f"[WARN] {url.rsplit('/', 1)[-1]} attempt {attempt}/{RETRIES}: "
              f"HTTP {r.status_code}, {len(r.content)}B", flush=True)
        time.sleep(10 * attempt)
    return None


def to_float(cell) -> float | None:
    if cell is None:
        return None
    s = str(cell).strip().replace(" ", " ")
    if "\n" in s or not s or s in {"-", "--", "*", "P,E.T"}:
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_date_token(tok: str) -> str | None:
    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{2,4})$", tok.strip())
    if not m:
        return None
    d, mo, y = (int(g) for g in m.groups())
    if y < 100:
        y += 2000
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


def clean_name(cell: str) -> str:
    s = re.sub(r"\s+", " ", str(cell).replace("\n", " ")).strip()
    m = NAME_YEAR_RE.match(s)
    if m:
        s = m.group(1).strip()
    s = re.sub(r"[^A-Z' \-]", "", s.upper()).strip()
    return re.sub(r"\s+", " ", s)


def is_aggregate(name: str) -> bool:
    return (not name) or any(tok in name for tok in AGG_TOKENS)


def big_table(page):
    best = None
    for t in page.extract_tables():
        if len(t) >= 10 and len(t[0]) >= 12:
            if best is None or len(t) * len(t[0]) > len(best) * len(best[0]):
                best = t
    return best


def parse_apports(page):
    """Inflow/outflow page. Returns (bulletin_date, flow_date, rows)."""
    text = page.extract_text() or ""
    m = re.search(r"du\s+(\d{1,2}-\d{1,2}-\d{2,4})\s+observ\w*\s+le\s+"
                  r"(\d{1,2}-\d{1,2}-\d{2,4})", text)
    flow_date = parse_date_token(m.group(1)) if m else None
    bull_date = parse_date_token(m.group(2)) if m else None
    table = big_table(page)
    rows: list[tuple[str, str, str, float]] = []
    if table is None:
        return bull_date, flow_date, rows
    ncols = len(table[0])
    pct_col, stock_col = (13, 14) if ncols >= 17 else (None, 13)
    for row in table:
        name = clean_name(row[0] or "")
        if is_aggregate(name):
            continue
        inflow = to_float(row[1])
        outflow = to_float(row[8]) if len(row) > 8 else None
        pct = to_float(row[pct_col]) if pct_col is not None and len(row) > pct_col else None
        stock = to_float(row[stock_col]) if len(row) > stock_col else None
        if inflow is not None and flow_date:
            rows.append((flow_date, name, "inflow_day_mcm", inflow))
        if outflow is not None and flow_date:
            rows.append((flow_date, name, "outflow_day_mcm", outflow))
        if pct is not None and bull_date and 0 <= pct <= 200:
            rows.append((bull_date, name, "storage_pct", pct))
        if stock is not None and bull_date:
            rows.append((bull_date, name, "storage_p1_mcm", stock))
    return bull_date, flow_date, rows


def parse_situation(page):
    """Hydraulic situation page. Returns (bulletin_date, rows)."""
    text = page.extract_text() or ""
    m = re.search(r"Journ\w*\s+du\s*:?\s*(\d{1,2}-\d{1,2}-\d{2,4})", text)
    bull_date = parse_date_token(m.group(1)) if m else None
    table = big_table(page)
    rows: list[tuple[str, str, str, float]] = []
    if table is None or len(table[0]) < 26:
        return bull_date, rows
    for row in table:
        name = clean_name(row[0] or "")
        if is_aggregate(name):
            continue
        if to_float(row[P2["cote_m"]]) is None and to_float(row[P2["volume_mcm"]]) is None:
            continue
        for var, col in (("water_level_m", P2["cote_m"]),
                         ("storage_mcm", P2["volume_mcm"]),
                         ("storage_useful_mcm", P2["volume_utilisable_mcm"]),
                         ("withdrawal_day_mcm", P2["soutirage_day_mcm"]),
                         ("precipitation_mm", P2["pluie_mm"]),
                         ("salinity_gl", P2["salinite_gl"])):
            v = to_float(row[col])
            if v is not None and bull_date:
                rows.append((bull_date, name, var, v))
    return bull_date, rows


def parse_bulletin(path: Path) -> tuple[str | None, list[dict]]:
    with pdfplumber.open(path) as pdf:
        apports_page = situation_page = None
        for p in pdf.pages:
            head = (p.extract_text() or "")[:400]
            if apports_page is None and "APP" in head and "LACHERS" in head.upper():
                apports_page = p
            elif situation_page is None and "Situation Hydraulique" in head:
                situation_page = p
        if apports_page is None and len(pdf.pages) >= 1:
            apports_page = pdf.pages[0]
        if situation_page is None and len(pdf.pages) >= 2:
            situation_page = pdf.pages[1]
        p1_date = p2_date = None
        rows: list[tuple[str, str, str, float]] = []
        if apports_page is not None:
            p1_date, _flow, r1 = parse_apports(apports_page)
            rows.extend(r1)
        if situation_page is not None:
            p2_date, r2 = parse_situation(situation_page)
            rows.extend(r2)
    date = p1_date or p2_date
    out = [{"obs_date": d, "reservoir_id": n, "variable": v, "value": f"{val:g}"}
           for d, n, v, val in rows]
    return date, out


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
            compare = [c for c in columns if c not in ("fetched_at", "bulletin_file")]
            if {c: existing[key][c] for c in compare} == {c: norm[c] for c in compare}:
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


def candidate_urls(day: date_cls) -> list[tuple[str, str]]:
    """Both URL patterns for one date; day/month are not zero-padded."""
    token = f"{day.day}-{day.month}-{day.year}"
    return [(f"BARRAGES-{token}.pdf", f"{UPLOADS}/BARRAGES-{token}.pdf"),
            (f"{token}.pdf", f"{UPLOADS}/{token}.pdf")]


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    log = {"started_at": fetched_at, "source": "tunisia/onagri",
           "listing": LISTING, "output_dir": str(OUTPUT_DIR),
           "status": "started", "errors": []}
    try:
        today = datetime.now(timezone.utc).date()
        probed = 0
        found: list[tuple[str, Path]] = []
        for back in range(PROBE_DAYS):
            day = today - timedelta(days=back)
            for name, url in candidate_urls(day):
                dest = RAW_DIR / str(day.year) / name
                if dest.exists() and dest.stat().st_size > 1024:
                    found.append((name, dest))
                    break
                probed += 1
                r = get_pdf(url)
                if r is None:
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(r.content)
                print(f"[SAVE] {dest} ({len(r.content)}B)", flush=True)
                found.append((name, dest))
                break

        log["urls_probed"] = probed
        log["bulletins_present"] = len(found)
        if not found:
            log["status"] = "no_new_bulletin"
            log["note"] = ("Publication dormant since 2025-10-10; probe found "
                           "nothing in the recent window (expected).")
            log["finished_at"] = utc_now_iso()
            append_run_log(log)
            print("[OK] no bulletin published in the probed window", flush=True)
            return 0

        all_rows: list[dict] = []
        parsed = 0
        for name, path in found:
            try:
                date, rows = parse_bulletin(path)
            except Exception as exc:  # noqa: BLE001 - one bad PDF must not kill the run
                print(f"[WARN] parse failed for {name}: {exc!r}", flush=True)
                continue
            if not date or not rows:
                print(f"[WARN] {name}: date={date} rows={len(rows)}", flush=True)
                continue
            parsed += 1
            for row in rows:
                row["bulletin_file"] = name
                row["fetched_at"] = fetched_at
            all_rows.extend(rows)

        added = updated = 0
        if all_rows:
            added, updated = merge_csv(TS_DIR / "tunisia_onagri_observations.csv",
                                       OBS_COLUMNS, all_rows,
                                       ["obs_date", "reservoir_id", "variable"])
        log.update({"status": "ok", "bulletins_parsed": parsed,
                    "values_parsed": len(all_rows), "rows_added": added,
                    "rows_updated": updated, "finished_at": utc_now_iso()})
        append_run_log(log)
        print(f"[OK] {parsed} bulletin(s) parsed, {len(all_rows)} values, "
              f"{added} added / {updated} updated", flush=True)
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

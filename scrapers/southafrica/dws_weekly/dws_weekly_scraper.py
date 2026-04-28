"""
South Africa DWS Weekly State of the Reservoirs scraper.

Each run produces ONE snapshot CSV for the most recent published
bulletin. Older snapshot CSVs in the output folder are removed so the
repo always carries exactly the latest scrape — historical accumulation
happens in the user's local pipeline, not in this repo.

Bulletin source: https://www.dws.gov.za/drought/docs/Weekly{YYYYMMDD}.pdf
    (filename date = report Monday)

Cadence rationale: DWS publishes one bulletin per Monday (sometimes
shifted by holidays). The GitHub Actions workflow runs weekly on
Tuesday 06:00 UTC, comfortably after the publish window.

Output layout (committed to git):
    data/southafrica/dws_weekly/
      timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv   (current snapshot only)
      metadata/southafrica_dws_reservoirs.csv            (one row per reservoir)
      run_logs/{run_date}_summary.json

The PDF cache lives at `pdfs/` but is gitignored (re-downloaded as
needed per run).

Usage:
    python3 dws_weekly_scraper.py                # current latest week
    DWS_TARGET_DATE=2026-04-20 python3 dws_weekly_scraper.py   # specific Monday
    DWS_KEEP_OLD=1 python3 dws_weekly_scraper.py # keep prior dated snapshots
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pdfplumber
import requests

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.5 Safari/605.1.15"
)
TIMEOUT = 60
ARCHIVE_TEMPLATES = [
    "https://www.dws.gov.za/drought/docs/Weekly{d}.pdf",
    "https://www.dws.gov.za/drought/docs/Weekly-{d}.pdf",
    "https://www.dws.gov.za/drought/docs/Weekly%20{d}.pdf",
]
PLACEHOLDER_MAX_BYTES = 50_000  # real bulletins are 700-900 KB; placeholder is ~1.4 KB
LOOKBACK_WEEKS = 8  # how many Mondays back to scan for the most recent real bulletin

OUT_BASE = Path(os.environ.get("OUTPUT_DIR") or
                Path(__file__).resolve().parents[3] / "data" / "southafrica" / "dws_weekly")
PDF_DIR = OUT_BASE / "pdfs"
TS_DIR = OUT_BASE / "timeseries"
META_DIR = OUT_BASE / "metadata"
LOG_DIR = OUT_BASE / "run_logs"
META_PATH = META_DIR / "southafrica_dws_reservoirs.csv"

TS_COLUMNS = [
    "date", "station_id", "reservoir", "river", "wma", "prov", "wss",
    "fsc_mcm", "water_mcm", "pct_last_year", "pct_last_week", "pct_full",
    "district_mun",
]


def env_date(name: str) -> Optional[date]:
    v = os.environ.get(name, "").strip()
    if not v:
        return None
    return datetime.strptime(v, "%Y-%m-%d").date()


def most_recent_monday(today: date) -> date:
    return today - timedelta(days=today.weekday())


def find_latest_bulletin(session: requests.Session, max_back_weeks: int = LOOKBACK_WEEKS) -> Optional[tuple[date, bytes, str]]:
    """Scan backwards from this week's Monday for the most recent real PDF.

    Returns (monday, pdf_bytes, source_url) or None if no bulletin found
    within the lookback window.
    """
    today = datetime.now(tz=timezone.utc).date()
    monday = most_recent_monday(today)
    for offset in range(max_back_weeks):
        target = monday - timedelta(days=7 * offset)
        yyyymmdd = target.strftime("%Y%m%d")
        for tmpl in ARCHIVE_TEMPLATES:
            url = tmpl.format(d=yyyymmdd)
            try:
                r = session.get(url, timeout=TIMEOUT)
            except requests.RequestException:
                continue
            if r.status_code != 200 or not r.content[:5].startswith(b"%PDF-"):
                continue
            if len(r.content) < PLACEHOLDER_MAX_BYTES:
                continue
            return target, r.content, url
    return None


# ---------- PDF parsing (single-bulletin) ----------

DATE_IN_HEADER_RE = re.compile(r"(20\d{2})\s*[-/ ]?\s*(\d{2})\s*[-/ ]?\s*(\d{2})")
STATION_RE = re.compile(r"^([A-Z]\d[A-Z]\d{3})")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def _strip_hash(x: str) -> str:
    return re.sub(r"^#\s*", "", str(x or "").strip())


def _to_float(x: str) -> Optional[float]:
    x = _strip_hash(x)
    if not x:
        return None
    try:
        return float(x)
    except ValueError:
        return None


def _find_header_row(table: list[list]) -> Optional[int]:
    for i, row in enumerate(table[:40]):
        joined = " | ".join(_norm(c) for c in row if c is not None)
        if "station" in joined and "reservoir" in joined:
            return i
    return None


def _canonicalize_columns(header: list[str], yyyymmdd: str) -> dict[int, str]:
    mapping: dict[int, str] = {}
    pct_full_candidates: list[tuple[int, int]] = []
    for idx, raw in enumerate(header):
        nc = _norm(raw)
        if nc == "station":
            mapping[idx] = "station_id"
        elif nc == "reservoir":
            mapping[idx] = "reservoir"
        elif nc == "river":
            mapping[idx] = "river"
        elif nc == "wss":
            mapping[idx] = "wss"
        elif "distr" in nc and "mun" in nc:
            mapping[idx] = "district_mun"
        elif nc == "wma" or nc.startswith("wma/") or ("wma" in nc and "count" in nc):
            mapping[idx] = "wma"
        elif nc == "prov" or nc.startswith("prov/") or ("prov" in nc and "count" in nc):
            mapping[idx] = "prov"
        elif "full supply capacity" in nc:
            mapping[idx] = "fsc_mcm"
        elif "water in dam" in nc:
            mapping[idx] = "water_mcm"
        elif "%full" in nc or "% full" in nc:
            if "last year" in nc or "last yr" in nc:
                mapping[idx] = "pct_last_year"
            elif "last week" in nc or "previous" in nc or "prev" in nc:
                mapping[idx] = "pct_last_week"
            elif "today" in nc:
                pct_full_candidates.append((100, idx))
            else:
                m = DATE_IN_HEADER_RE.search(nc)
                if m and "".join(m.groups()) == yyyymmdd:
                    pct_full_candidates.append((90, idx))
                else:
                    pct_full_candidates.append((10, idx))
    if pct_full_candidates:
        pct_full_candidates.sort(reverse=True)
        mapping[pct_full_candidates[0][1]] = "pct_full"
    return mapping


def _row_is_data(cells: list[str], station_idx: int) -> bool:
    if not cells or station_idx >= len(cells):
        return False
    cells = [(c or "").strip() for c in cells]
    if not re.match(r"^[A-Z]\d[A-Z]\d{3}", cells[station_idx]):
        return False
    joined = " ".join(cells).lower()
    if "subtotal" in joined or "grand total" in joined:
        return False
    return True


def parse_pdf(pdf_path: Path, yyyymmdd: str) -> list[dict]:
    out_rows: list[dict] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for tbl in tables or []:
                if not tbl:
                    continue
                hr = _find_header_row(tbl)
                if hr is None:
                    continue
                header = [(c or "").strip() for c in tbl[hr]]
                col_map = _canonicalize_columns(header, yyyymmdd)
                station_idx = next((i for i, n in col_map.items() if n == "station_id"), -1)
                if station_idx < 0:
                    continue
                for raw_row in tbl[hr + 1:]:
                    if raw_row is None:
                        continue
                    cells = [(c or "").strip().replace("\n", "") for c in raw_row]
                    if not _row_is_data(cells, station_idx):
                        continue
                    rec = {c: "" for c in TS_COLUMNS}
                    rec["date"] = yyyymmdd
                    for idx, name in col_map.items():
                        if idx >= len(cells):
                            continue
                        val = cells[idx]
                        if name == "station_id":
                            m = STATION_RE.match(val)
                            rec["station_id"] = m.group(1) if m else (val.split()[0] if val else "")
                        elif name in ("fsc_mcm", "water_mcm", "pct_last_year",
                                       "pct_last_week", "pct_full"):
                            f = _to_float(val)
                            rec[name] = "" if f is None else f"{f:g}"
                        else:
                            rec[name] = val
                    if rec["station_id"]:
                        out_rows.append(rec)
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in out_rows:
        if r["station_id"] in seen:
            continue
        seen.add(r["station_id"])
        deduped.append(r)
    return deduped


def write_snapshot(rows: list[dict], yyyymmdd: str, keep_old: bool) -> Path:
    TS_DIR.mkdir(parents=True, exist_ok=True)
    if not keep_old:
        for old in TS_DIR.glob("southafrica_dws_weekly_*.csv"):
            old.unlink()
    out_path = TS_DIR / f"southafrica_dws_weekly_{yyyymmdd}.csv"
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TS_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return out_path


def write_metadata(rows: list[dict]) -> int:
    META_DIR.mkdir(parents=True, exist_ok=True)
    meta_cols = ["station_id", "reservoir", "river", "wma", "prov", "wss",
                 "district_mun", "fsc_mcm"]
    with META_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=meta_cols, extrasaction="ignore")
        w.writeheader()
        for r in sorted(rows, key=lambda x: x["station_id"]):
            w.writerow({c: r.get(c, "") for c in meta_cols})
    return len(rows)


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    sess = requests.Session()
    sess.headers["User-Agent"] = UA

    target_date = env_date("DWS_TARGET_DATE")
    keep_old = bool(int(os.environ.get("DWS_KEEP_OLD", "0") or "0"))

    summary: dict = {
        "ran_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "target_date_override": target_date.isoformat() if target_date else None,
    }

    if target_date:
        yyyymmdd = target_date.strftime("%Y%m%d")
        url = ARCHIVE_TEMPLATES[0].format(d=yyyymmdd)
        r = sess.get(url, timeout=TIMEOUT)
        if r.status_code != 200 or not r.content[:5].startswith(b"%PDF-") or len(r.content) < PLACEHOLDER_MAX_BYTES:
            print(f"[dws-weekly] FAIL: no real bulletin at {url}")
            summary["status"] = "no-bulletin-at-target"
            (LOG_DIR / f"{datetime.now(tz=timezone.utc).date().isoformat()}_summary.json").write_text(
                json.dumps(summary, indent=2))
            return 1
        bulletin_date = target_date
        pdf_bytes = r.content
        source_url = url
    else:
        found = find_latest_bulletin(sess)
        if not found:
            print(f"[dws-weekly] FAIL: no real bulletin in past {LOOKBACK_WEEKS} Mondays")
            summary["status"] = "no-bulletin-in-lookback"
            (LOG_DIR / f"{datetime.now(tz=timezone.utc).date().isoformat()}_summary.json").write_text(
                json.dumps(summary, indent=2))
            return 1
        bulletin_date, pdf_bytes, source_url = found

    yyyymmdd = bulletin_date.strftime("%Y%m%d")
    pdf_path = PDF_DIR / f"Weekly{yyyymmdd}.pdf"
    pdf_path.write_bytes(pdf_bytes)
    print(f"[dws-weekly] using bulletin {bulletin_date}  ({len(pdf_bytes):,} bytes)")
    print(f"[dws-weekly] source: {source_url}")

    rows = parse_pdf(pdf_path, yyyymmdd)
    if not rows:
        print(f"[dws-weekly] FAIL: parsed 0 reservoir rows from {pdf_path.name}")
        summary["status"] = "parse-empty"
        summary["bulletin_date"] = yyyymmdd
        (LOG_DIR / f"{datetime.now(tz=timezone.utc).date().isoformat()}_summary.json").write_text(
            json.dumps(summary, indent=2))
        return 1

    snapshot_path = write_snapshot(rows, yyyymmdd, keep_old=keep_old)
    n_meta = write_metadata(rows)

    summary.update({
        "status": "ok",
        "bulletin_date": yyyymmdd,
        "source_url": source_url,
        "rows": len(rows),
        "stations": n_meta,
        "snapshot_path": snapshot_path.relative_to(OUT_BASE.parent.parent.parent).as_posix(),
    })
    (LOG_DIR / f"{datetime.now(tz=timezone.utc).date().isoformat()}_summary.json").write_text(
        json.dumps(summary, indent=2))
    print(f"[dws-weekly] OK: {len(rows)} reservoirs -> {snapshot_path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

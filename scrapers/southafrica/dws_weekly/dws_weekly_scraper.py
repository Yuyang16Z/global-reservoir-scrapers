"""
South Africa DWS Weekly State of the Reservoirs scraper.

Downloads weekly bulletin PDFs from
    https://www.dws.gov.za/drought/docs/Weekly{YYYYMMDD}.pdf
extracts the reservoir-state table via pdfplumber, and appends new rows
to a cumulative long-format timeseries CSV.

Cadence rationale: DWS publishes one bulletin per Monday (sometimes
shifted by holidays). Source updates weekly → no information gain from
polling more often. Workflow is scheduled weekly on Tuesday UTC, after
DWS's Monday publish window.

Per the dataset's data-location-routing rule, this scraper lives in
the git repo (ephemeral weekly snapshots → committed back). The
companion `dws_verified_scraper.py` (one-shot 1922-onwards historical
archive) is kept LOCAL only and is NOT deployed to git.

Idempotency:
    - Only downloads PDFs that aren't already cached in `pdfs/`.
    - Only re-parses PDFs whose extracted dates aren't already in the
      timeseries CSV.
    - Skips placeholder PDFs (DWS returns ~1.4KB stub when a date
      hasn't been published yet).

Outputs:
    data/southafrica/dws_weekly/
      pdfs/Weekly{YYYYMMDD}.pdf         (raw PDFs, kept for audit)
      timeseries/timeseries_long.csv    (cumulative per-week per-dam rows)
      metadata/southafrica_dws_reservoirs.csv  (rebuilt every run)
      run_logs/{date}_summary.json

Backfill window:
    By default scans the last 24 months of Mondays. Override with
    DWS_START_DATE / DWS_END_DATE env vars (YYYY-MM-DD) for one-shot
    deeper backfill.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import pdfplumber
import requests

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.5 Safari/605.1.15"
)
TIMEOUT = 60
SOURCE_URL = "https://www.dws.gov.za/Hydrology/Weekly/Province.aspx"
ARCHIVE_TEMPLATES = [
    "https://www.dws.gov.za/drought/docs/Weekly{d}.pdf",
    "https://www.dws.gov.za/drought/docs/Weekly-{d}.pdf",
    "https://www.dws.gov.za/drought/docs/Weekly%20{d}.pdf",
]
PLACEHOLDER_MAX_BYTES = 50_000  # real bulletins are 700-900 KB; placeholder is ~1.4 KB

OUT_BASE = Path(os.environ.get("OUTPUT_DIR") or
                Path(__file__).resolve().parents[3] / "data" / "southafrica" / "dws_weekly")
PDF_DIR = OUT_BASE / "pdfs"
TS_DIR = OUT_BASE / "timeseries"
META_DIR = OUT_BASE / "metadata"
LOG_DIR = OUT_BASE / "run_logs"

TS_PATH = TS_DIR / "timeseries_long.csv"
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


def mondays_between(start: date, end: date) -> list[date]:
    d = start - timedelta(days=start.weekday())  # back up to its Monday
    out = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


@dataclass
class DownloadResult:
    monday: date
    yyyymmdd: str
    status: str          # "downloaded" / "cached" / "no-bulletin" / "fail"
    path: Optional[Path]
    bytes: int
    url: str


def download_one(monday: date, session: requests.Session, force: bool = False) -> DownloadResult:
    yyyymmdd = monday.strftime("%Y%m%d")
    out_path = PDF_DIR / f"Weekly{yyyymmdd}.pdf"
    if out_path.exists() and not force:
        return DownloadResult(monday, yyyymmdd, "cached", out_path, out_path.stat().st_size, "")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    last_url = ""
    for tmpl in ARCHIVE_TEMPLATES:
        url = tmpl.format(d=yyyymmdd)
        last_url = url
        try:
            r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
        except requests.RequestException:
            continue
        if r.status_code != 200:
            continue
        content = r.content
        if not content[:5].startswith(b"%PDF-"):
            continue  # placeholder/HTML
        if len(content) < PLACEHOLDER_MAX_BYTES:
            continue  # real PDFs are >700KB; this is the DWS "no bulletin" stub
        out_path.write_bytes(content)
        return DownloadResult(monday, yyyymmdd, "downloaded", out_path, len(content), url)
    return DownloadResult(monday, yyyymmdd, "no-bulletin", None, 0, last_url)


# ---------- PDF parsing ----------
# Mirrors the locally-developed extract_weekly_tables_pdfplumber.py logic but
# streamlined: parse all pages, find the reservoir-state table (header has
# "Station" + "Reservoir"), extract rows, normalize columns.

DATE_IN_HEADER_RE = re.compile(r"(20\d{2})\s*[-/ ]?\s*(\d{2})\s*[-/ ]?\s*(\d{2})")


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


def _canonicalize_columns(header: list[str], weekly_yyyymmdd: str) -> dict[int, str]:
    """Map column-index -> canonical name."""
    mapping: dict[int, str] = {}
    pct_full_candidates: list[tuple[int, int]] = []  # (priority, idx)

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
                if m and "".join(m.groups()) == weekly_yyyymmdd:
                    pct_full_candidates.append((90, idx))
                else:
                    pct_full_candidates.append((10, idx))
    if pct_full_candidates:
        pct_full_candidates.sort(reverse=True)
        mapping[pct_full_candidates[0][1]] = "pct_full"
    return mapping


def _row_is_data(cells: list[str], station_idx: int) -> bool:
    """Distinguish data rows from group/header/footer rows.

    station_idx points to the column where station_id should appear.
    Real data rows have a value matching pattern A1R001 there.
    """
    if not cells or station_idx >= len(cells):
        return False
    cells = [(c or "").strip() for c in cells]
    val = cells[station_idx]
    if not re.match(r"^[A-Z]\d[A-Z]\d{3}", val):
        return False
    joined = " ".join(cells).lower()
    if "subtotal" in joined or "grand total" in joined:
        return False
    return True


STATION_RE = re.compile(r"^([A-Z]\d[A-Z]\d{3})")


def parse_pdf(pdf_path: Path, weekly_yyyymmdd: str) -> list[dict]:
    """Extract reservoir-state rows from one DWS Weekly PDF."""
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
                col_map = _canonicalize_columns(header, weekly_yyyymmdd)
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
                    rec["date"] = weekly_yyyymmdd
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
    # Within one PDF, the same station_id may appear in multiple page-tables.
    # Keep the first occurrence (typically the canonical reservoir line).
    seen = set()
    deduped = []
    for r in out_rows:
        if r["station_id"] in seen:
            continue
        seen.add(r["station_id"])
        deduped.append(r)
    return deduped


# ---------- top-level orchestration ----------

def load_existing_dates() -> set[str]:
    if not TS_PATH.exists():
        return set()
    with TS_PATH.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        return {row["date"] for row in rdr if row.get("date")}


def append_rows(rows: list[dict]) -> None:
    if not rows:
        return
    TS_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new = not TS_PATH.exists()
    with TS_PATH.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TS_COLUMNS, extrasaction="ignore")
        if is_new:
            w.writeheader()
        w.writerows(rows)


def rebuild_metadata() -> int:
    """Build per-station metadata from the current cumulative timeseries.

    Picks the most-recent non-empty values for reservoir, river, wma, prov,
    wss, district_mun, and the median of fsc_mcm (capacity is constant; we
    use median to absorb rounding drift).
    """
    if not TS_PATH.exists():
        return 0
    rows: dict[str, dict] = {}
    with TS_PATH.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            sid = r.get("station_id", "")
            if not sid:
                continue
            entry = rows.setdefault(sid, {
                "station_id": sid,
                "reservoir": "", "river": "", "wma": "", "prov": "", "wss": "",
                "district_mun": "",
                "fsc_values": [],
                "first_date": r["date"], "last_date": r["date"], "n_obs": 0,
            })
            entry["n_obs"] += 1
            entry["last_date"] = max(entry["last_date"] or "", r["date"] or "")
            entry["first_date"] = min(entry["first_date"] or r["date"], r["date"] or entry["first_date"])
            for col in ("reservoir", "river", "wma", "prov", "wss", "district_mun"):
                v = (r.get(col) or "").strip()
                if v and not entry[col]:
                    entry[col] = v
            try:
                fsc = float(r.get("fsc_mcm") or "")
                entry["fsc_values"].append(fsc)
            except ValueError:
                pass

    META_PATH.parent.mkdir(parents=True, exist_ok=True)
    with META_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_id", "reservoir", "river", "wma", "prov", "wss",
                    "district_mun", "fsc_mcm_median", "n_observations",
                    "first_date", "last_date"])
        for sid in sorted(rows):
            e = rows[sid]
            fsc = e["fsc_values"]
            fsc_med = (sorted(fsc)[len(fsc) // 2] if fsc else "")
            w.writerow([sid, e["reservoir"], e["river"], e["wma"], e["prov"],
                        e["wss"], e["district_mun"], fsc_med, e["n_obs"],
                        e["first_date"], e["last_date"]])
    return len(rows)


def main() -> int:
    today = datetime.now(tz=timezone.utc).date()
    end = env_date("DWS_END_DATE") or today
    start = env_date("DWS_START_DATE") or (end - timedelta(days=730))  # 24 months default

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[dws-weekly] window: {start} -> {end}")
    targets = mondays_between(start, end)
    print(f"[dws-weekly] {len(targets)} Mondays to check")

    have_dates = load_existing_dates()
    sess = requests.Session()
    sess.headers["User-Agent"] = UA

    results: list[DownloadResult] = []
    new_rows: list[dict] = []
    parse_failures: list[str] = []

    for i, monday in enumerate(targets, 1):
        # Skip if already in timeseries AND PDF cached
        yyyymmdd = monday.strftime("%Y%m%d")
        pdf_path = PDF_DIR / f"Weekly{yyyymmdd}.pdf"
        skip = (yyyymmdd in have_dates) and pdf_path.exists()
        if skip:
            results.append(DownloadResult(monday, yyyymmdd, "cached", pdf_path,
                                          pdf_path.stat().st_size, ""))
            continue

        res = download_one(monday, sess)
        results.append(res)
        if res.status not in ("downloaded", "cached"):
            continue
        if yyyymmdd in have_dates:
            continue  # already parsed

        try:
            rows = parse_pdf(res.path, yyyymmdd)
        except Exception as exc:
            parse_failures.append(f"{yyyymmdd}: {exc!r}")
            continue
        if not rows:
            parse_failures.append(f"{yyyymmdd}: no reservoir rows extracted")
            continue
        new_rows.extend(rows)
        print(f"  [{i}/{len(targets)}] {monday}: {res.status:10s} +{len(rows)} rows")
        # politeness delay between live downloads
        if res.status == "downloaded":
            time.sleep(1.0)

    append_rows(new_rows)
    n_stations = rebuild_metadata()

    summary = {
        "ran_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "window": [start.isoformat(), end.isoformat()],
        "mondays_checked": len(targets),
        "downloaded": sum(1 for r in results if r.status == "downloaded"),
        "cached": sum(1 for r in results if r.status == "cached"),
        "no_bulletin": sum(1 for r in results if r.status == "no-bulletin"),
        "fail": sum(1 for r in results if r.status == "fail"),
        "new_rows": len(new_rows),
        "total_dates_in_ts": len(load_existing_dates()),
        "stations_in_metadata": n_stations,
        "parse_failures": parse_failures,
    }
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{today.isoformat()}_summary.json"
    log_path.write_text(json.dumps(summary, indent=2))
    print(f"\n[dws-weekly] done: +{len(new_rows)} new rows, "
          f"{n_stations} stations in metadata, "
          f"{summary['downloaded']} new PDFs, "
          f"{summary['no_bulletin']} dates with no bulletin")
    print(f"[dws-weekly] log -> {log_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

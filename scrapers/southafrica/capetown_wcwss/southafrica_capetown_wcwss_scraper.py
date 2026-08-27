"""South Africa - City of Cape Town WCWSS weekly dam storage scraper.

Source:
- https://resource.capetown.gov.za/documentcentre/Documents/
  City%20research%20reports%20and%20review/damlevels.pdf
- https://web1.capetown.gov.za/web1/newsandnotices/site-media/documents/damlevels.pdf
  ("Weekly Water Dashboard", City of Cape Town)

Why this source exists in the repo:
- The national DWS weekly bulletin stopped publishing after 2026-06-29: the
  official pages answer HTTP 403 and every downstream aggregator, including
  the reservoirs.earth mirror this project uses, is frozen on that same date.
  The City of Cape Town publishes its own weekly dashboard from its own
  readings and is still current, so it keeps six Western Cape Water Supply
  System dams flowing while DWS is dark. It covers those six dams only - it is
  a complement to the national bulletin, not a replacement for it.

Known quirks:
- OVERWRITE-PRONE: one stable URL always holds the newest dashboard, so a week
  that is not captured is lost.
- The observation date is the dashboard's own printed date in the storage table
  header ("03 August 2026"), never the fetch date.
- Page 2 carries the dam table: name, capacity in Ml, current %, previous
  week %, then the same week's % in each of the four preceding years. Only the
  current and previous-week columns are treated as observations; the
  prior-year columns are historical context printed for comparison and are
  recorded separately so they are never mistaken for this week's reading.
- Capacity is published in megalitres; storage volume is derived as
  capacity_ml * percent / 100 and reported in Ml, not invented in Mm3.
- "VOELVLEI" is printed with a diaeresis; matching is accent-insensitive.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/damlevels_<observation-date>.pdf
  metadata/southafrica_capetown_wcwss_reservoirs.csv
  timeseries/southafrica_capetown_wcwss_weekly.csv   (merged, idempotent)
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
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
META_DIR = OUTPUT_DIR / "metadata"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

PRIMARY_PDF_URL = (
    "https://resource.capetown.gov.za/documentcentre/Documents/"
    "City%20research%20reports%20and%20review/damlevels.pdf"
)
FALLBACK_PDF_URL = (
    "https://web1.capetown.gov.za/web1/newsandnotices/"
    "site-media/documents/damlevels.pdf"
)
PDF_URLS = (PRIMARY_PDF_URL, FALLBACK_PDF_URL)
SOURCE_PAGE = "https://web1.capetown.gov.za/web1/newsandnotices/Home/Release/dam-levels"
SOURCE_AGENCY = "City of Cape Town"
TIMEOUT = 120
CONNECT_TIMEOUT = 15
PDF_MIN_BYTES = 50_000
REQUEST_ATTEMPTS = 3
REQUEST_BACKOFFS = (2, 8, 20)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

# Printed name -> (stable id, English name). The dashboard's six WCWSS dams.
DAMS = {
    "BERG RIVER": ("ZA_CCT_BERG_RIVER", "Berg River"),
    "STEENBRAS LOWER": ("ZA_CCT_STEENBRAS_LOWER", "Steenbras Lower"),
    "STEENBRAS UPPER": ("ZA_CCT_STEENBRAS_UPPER", "Steenbras Upper"),
    "THEEWATERSKLOOF": ("ZA_CCT_THEEWATERSKLOOF", "Theewaterskloof"),
    "VOELVLEI": ("ZA_CCT_VOELVLEI", "Voelvlei"),
    "WEMMERSHOEK": ("ZA_CCT_WEMMERSHOEK", "Wemmershoek"),
}

TS_COLUMNS = ["measurement_date", "reservoir_id", "reservoir_name",
              "capacity_ml", "storage_pct", "storage_ml", "reading_kind",
              "fetched_at"]
META_COLUMNS = ["reservoir_id", "reservoir_name", "reservoir_name_en", "country",
                "admin_unit", "river", "basin", "capacity_ml", "source_agency",
                "source_url", "data_type", "last_updated"]

MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], 1)}
DATE_RE = re.compile(r"\b(\d{1,2})\s+([A-Z][a-z]+)\s+(\d{4})\b")
NUM = r"-?\d[\d,]*(?:\.\d+)?"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, META_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def is_source_unavailable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code >= 500
    return False


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def fetch_pdf() -> tuple[bytes, str]:
    last_exc: Exception | None = None
    for url_index, url in enumerate(PDF_URLS):
        # The legacy document-centre host is retained as the preferred canonical
        # URL, but a connect timeout should move promptly to the City's official
        # news-and-notices media endpoint.
        attempts = 1 if url_index == 0 else REQUEST_ATTEMPTS
        for attempt in range(1, attempts + 1):
            try:
                r = requests.get(
                    url,
                    headers=HEADERS,
                    timeout=(CONNECT_TIMEOUT, TIMEOUT),
                )
                r.raise_for_status()
                if len(r.content) < PDF_MIN_BYTES or not r.content.startswith(b"%PDF"):
                    raise RuntimeError(
                        f"dashboard URL did not return a plausible PDF ({len(r.content)} bytes)"
                    )
                return r.content, url
            except requests.RequestException as exc:
                last_exc = exc
                print(
                    f"[WARN] dashboard fetch {url} attempt {attempt}/{attempts}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                if attempt < attempts:
                    time.sleep(
                        REQUEST_BACKOFFS[min(attempt - 1, len(REQUEST_BACKOFFS) - 1)]
                    )
            except RuntimeError as exc:
                last_exc = exc
                print(
                    f"[WARN] dashboard validation {url} attempt {attempt}/{attempts}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                if attempt < attempts:
                    time.sleep(
                        REQUEST_BACKOFFS[min(attempt - 1, len(REQUEST_BACKOFFS) - 1)]
                    )
    assert last_exc is not None
    raise last_exc


def to_f(raw: str) -> float | None:
    try:
        return float(str(raw).replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def dam_rows_present(text: str) -> bool:
    for line in text.split("\n"):
        flat = strip_accents(re.sub(r"\s+", " ", line)).upper().strip()
        if any(flat.startswith(p) for p in DAMS):
            return True
    return False


def parse_dashboard(pdf_path: Path, fetched_at: str) -> tuple[str | None, list[dict]]:
    # The caption "MAJOR DAMS" also appears on the summary page, so pick the
    # page that actually carries dam rows rather than the first textual match.
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        pages = [p.extract_text() or "" for p in pdf.pages]
    for t in pages:
        if dam_rows_present(t):
            text = t
            break
    if not text:
        text = "\n".join(pages)

    obs_date = prev_date = None
    for line in text.split("\n"):
        if "Previous week" in line or re.search(r"^\s*Ml\b", line):
            m = DATE_RE.search(line)
            if m and m.group(2) in MONTHS:
                obs_date = (f"{int(m.group(3)):04d}-{MONTHS[m.group(2)]:02d}-"
                            f"{int(m.group(1)):02d}")
                break
    if obs_date is None:
        m = DATE_RE.search(text)
        if m and m.group(2) in MONTHS:
            obs_date = (f"{int(m.group(3)):04d}-{MONTHS[m.group(2)]:02d}-"
                        f"{int(m.group(1)):02d}")
    if obs_date:
        d = datetime.strptime(obs_date, "%Y-%m-%d")
        prev_date = (d.toordinal() - 7)
        prev_date = datetime.fromordinal(prev_date).strftime("%Y-%m-%d")

    rows: list[dict] = []
    for line in text.split("\n"):
        flat = strip_accents(re.sub(r"\s+", " ", line)).upper().strip()
        for printed, (rid, name_en) in DAMS.items():
            if not flat.startswith(printed):
                continue
            nums = re.findall(NUM, line[len(printed):])
            vals = [to_f(n) for n in nums]
            vals = [v for v in vals if v is not None]
            if len(vals) < 2:
                continue
            capacity, pct_now = vals[0], vals[1]
            pct_prev = vals[2] if len(vals) > 2 else None
            for date, pct, kind in ((obs_date, pct_now, "current"),
                                    (prev_date, pct_prev, "previous_week")):
                if not date or pct is None or not (0 <= pct <= 130):
                    continue
                rows.append({
                    "measurement_date": date,
                    "reservoir_id": rid,
                    "reservoir_name": name_en,
                    "capacity_ml": f"{capacity:g}",
                    "storage_pct": f"{pct:g}",
                    "storage_ml": f"{capacity * pct / 100:.1f}",
                    "reading_kind": kind,
                    "fetched_at": fetched_at,
                })
            break
    return obs_date, rows


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
        old = existing.get(key)
        if old is None:
            added += 1
        else:
            # a later dashboard's "current" reading supersedes the earlier
            # dashboard's "previous_week" value for the same date
            if old.get("reading_kind") == "current" and norm["reading_kind"] != "current":
                continue
            compare = [c for c in columns if c != "fetched_at"]
            if {c: old[c] for c in compare} == {c: norm[c] for c in compare}:
                continue
            updated += 1
        existing[key] = norm
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=columns,
            extrasaction="ignore",
            lineterminator="\n",
        )
        w.writeheader()
        for key in sorted(existing):
            w.writerow(existing[key])
    print(f"[SAVE] {path} total={len(existing)} added={added} updated={updated}",
          flush=True)
    return added, updated


def save_summary(log: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    p = RUN_LOG_DIR / f"{utc_stamp()}_summary.json"
    with p.open("w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {p}", flush=True)


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    log = {"started_at": fetched_at, "source": "southafrica/capetown_wcwss",
           "urls": list(PDF_URLS), "output_dir": str(OUTPUT_DIR), "status": "started",
           "errors": []}
    try:
        body, fetched_url = fetch_pdf()
        log["fetched_url"] = fetched_url
        tmp = RAW_DIR / f"damlevels_{utc_stamp()}.pdf"
        tmp.write_bytes(body)
        obs_date, rows = parse_dashboard(tmp, fetched_at)
        log["observation_date"] = obs_date
        log["rows_parsed"] = len(rows)
        if not obs_date:
            raise RuntimeError("no dashboard date parsed")
        if not rows:
            raise RuntimeError("no dam rows parsed from the dashboard table")

        final = RAW_DIR / f"damlevels_{obs_date}.pdf"
        if final.exists():
            tmp.unlink()
        else:
            tmp.rename(final)
        print(f"[SAVE] {final} ({len(body)}B)", flush=True)

        added, updated = merge_csv(TS_DIR / "southafrica_capetown_wcwss_weekly.csv",
                                   TS_COLUMNS, rows,
                                   ["measurement_date", "reservoir_id"])
        meta = []
        seen = set()
        for r in rows:
            if r["reservoir_id"] in seen:
                continue
            seen.add(r["reservoir_id"])
            meta.append({
                "reservoir_id": r["reservoir_id"],
                "reservoir_name": r["reservoir_name"],
                "reservoir_name_en": r["reservoir_name"],
                "country": "South Africa",
                "admin_unit": "Western Cape",
                "river": "",
                "basin": "Western Cape Water Supply System",
                "capacity_ml": r["capacity_ml"],
                "source_agency": SOURCE_AGENCY,
                "source_url": SOURCE_PAGE,
                "data_type": "in_situ",
                "last_updated": fetched_at,
            })
        merge_csv(META_DIR / "southafrica_capetown_wcwss_reservoirs.csv",
                  META_COLUMNS, meta, ["reservoir_id"])
        log.update({"status": "ok", "dams": len(meta), "rows_added": added,
                    "rows_updated": updated, "raw_file": final.name,
                    "finished_at": utc_now_iso()})
        save_summary(log)
        print(f"[OK] {obs_date}: {len(meta)} dams, {added} added / {updated} updated",
              flush=True)
        return 0
    except Exception as exc:  # noqa: BLE001 - scheduled job: log and signal
        if is_source_unavailable(exc):
            log["status"] = "source_unavailable"
            log["errors"].append({"message": str(exc),
                                  "error_type": exc.__class__.__name__})
            log["finished_at"] = utc_now_iso()
            save_summary(log)
            print(f"[WARN] Cape Town dashboard unavailable this run: {exc}",
                  file=sys.stderr)
            return 0
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        save_summary(log)
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

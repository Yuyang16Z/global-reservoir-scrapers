"""
South Africa DWS Weekly State of the Reservoirs scraper.

Each run finds the most recent published bulletin and writes one
dated snapshot CSV. Snapshots accumulate over time — every weekly
run adds the new week's file alongside prior weeks (no deletion).
The repo is the durable archive going forward; cumulative history
grows one file per run.

Bulletin source: https://www.dws.gov.za/drought/docs/Weekly{YYYYMMDD}.pdf
    (filename date = report Monday)

Cadence rationale: DWS publishes one bulletin per Monday (sometimes
shifted by holidays). The GitHub Actions workflow runs weekly on
Tuesday 06:00 UTC, comfortably after the publish window.

Output layout (committed to git):
    data/southafrica/dws_weekly/
      timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv   (one file per run)
      metadata/southafrica_dws_reservoirs.csv            (latest snapshot's reservoirs)
      run_logs/{run_date}_summary.json

Idempotent: if the bulletin for the target week is already in
`timeseries/` (file with matching YYYYMMDD), the scrape exits early
without re-parsing. Re-running on the same day produces no diff.

The PDF cache lives at `pdfs/` but is gitignored.

Usage:
    python3 dws_weekly_scraper.py                              # latest published week
    DWS_TARGET_DATE=2026-04-20 python3 dws_weekly_scraper.py   # specific Monday
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
MIRROR_URL = "https://reservoirs.earth/data/south-africa.json"
MIRROR_PAGE_ROOT = "https://reservoirs.earth/south-africa/reservoirs"
MIRROR_LICENSE = "https://creativecommons.org/licenses/by/4.0/"
MIRROR_MIN_ROWS = 150
MIRROR_MAX_AGE_DAYS = 21
MIRROR_HISTORY_WORKERS = 8

OUT_BASE = Path(os.environ.get("OUTPUT_DIR") or
                Path(__file__).resolve().parents[3] / "data" / "southafrica" / "dws_weekly")
PDF_DIR = OUT_BASE / "pdfs"
RAW_DIR = OUT_BASE / "raw"
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


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = UA
    retry = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _probe_pdf(session: requests.Session, url: str) -> tuple[Optional[bytes], dict]:
    probe = {"url": url}
    try:
        response = session.get(url, timeout=TIMEOUT)
    except requests.RequestException as exc:
        probe.update({"result": "request-error", "error": str(exc)})
        return None, probe

    is_pdf = response.content[:5] == b"%PDF-"
    is_real_pdf = (
        response.status_code == 200
        and is_pdf
        and len(response.content) >= PLACEHOLDER_MAX_BYTES
    )
    probe.update({
        "result": "real-pdf" if is_real_pdf else "rejected",
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type", ""),
        "bytes": len(response.content),
        "pdf_signature": is_pdf,
    })
    if probe["result"] == "real-pdf":
        return response.content, probe
    return None, probe


def find_latest_bulletin(
    session: requests.Session,
    max_back_weeks: int = LOOKBACK_WEEKS,
) -> tuple[Optional[tuple[date, bytes, str]], list[dict]]:
    """Scan backwards from this week's Monday for the most recent real PDF.

    Returns (monday, pdf_bytes, source_url) or None if no bulletin found
    within the lookback window.
    """
    probes: list[dict] = []
    today = datetime.now(tz=timezone.utc).date()
    monday = most_recent_monday(today)
    for offset in range(max_back_weeks):
        target = monday - timedelta(days=7 * offset)
        yyyymmdd = target.strftime("%Y%m%d")
        for tmpl in ARCHIVE_TEMPLATES:
            url = tmpl.format(d=yyyymmdd)
            pdf_bytes, probe = _probe_pdf(session, url)
            probes.append(probe)
            if pdf_bytes is not None:
                return (target, pdf_bytes, url), probes
    return None, probes


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
    import pdfplumber

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


# ---------- reservoirs.earth fallback (DWS-derived JSON) ----------

def _reservoir_key(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    ascii_name = re.sub(r"\b(dam|reservoir)\b", "", ascii_name.lower())
    return re.sub(r"[^a-z0-9]", "", ascii_name)


def _load_metadata_by_name() -> dict[str, dict]:
    if not META_PATH.exists():
        raise RuntimeError(f"metadata file is missing: {META_PATH}")
    with META_PATH.open(encoding="utf-8", newline="") as f:
        return {_reservoir_key(row["reservoir"]): row for row in csv.DictReader(f)}


def _history_reading(
    session: requests.Session,
    slug: str,
    target_date: date,
) -> Optional[dict]:
    """Read one older observation from a mirror reservoir page.

    The country JSON exposes only each reservoir's latest observation. When a
    small subset is newer than the national report date, its server-rendered
    history table supplies the observation for the common snapshot date.
    """
    url = f"{MIRROR_PAGE_ROOT}/{slug}"
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()

    # Next.js embeds the underlying readings with decimal precision. Prefer
    # that payload over the visible table, which rounds stored volume.
    target_iso = re.escape(target_date.isoformat())
    embedded = re.search(
        rf'\{{\\"reading_date\\":\\"{target_iso}\\",'
        rf'\\"volume_mcm\\":(null|-?\d+(?:\.\d+)?),'
        rf'\\"fill_percentage\\":(null|-?\d+(?:\.\d+)?)',
        response.text,
    )
    if embedded:
        volume_text, pct_text = embedded.groups()
        return {
            "date": target_date.isoformat(),
            "fill_percentage": None if pct_text == "null" else float(pct_text),
            "volume_mcm": None if volume_text == "null" else float(volume_text),
        }

    soup = BeautifulSoup(response.text, "html.parser")
    target_text = target_date.strftime("%-d %b %Y")
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 3 or cells[0].get_text(" ", strip=True) != target_text:
            continue
        pct_text = cells[1].get_text(" ", strip=True).replace("%", "").replace(",", "")
        volume_text = cells[2].get_text(" ", strip=True).replace(",", "")
        return {
            "date": target_date.isoformat(),
            "fill_percentage": _to_float(pct_text),
            "volume_mcm": _to_float(volume_text),
        }
    return None


def fetch_mirror_snapshot(
    session: requests.Session,
    requested_date: Optional[date] = None,
) -> tuple[date, list[dict], bytes, dict]:
    response = session.get(MIRROR_URL, timeout=TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    reservoirs = payload.get("reservoirs") if isinstance(payload, dict) else None
    if not isinstance(reservoirs, list) or len(reservoirs) < MIRROR_MIN_ROWS:
        raise RuntimeError("mirror JSON is missing the expected reservoir array")
    if payload.get("source") != "DWS":
        raise RuntimeError(f"unexpected mirror upstream source: {payload.get('source')!r}")

    latest_dates = [
        item.get("latest", {}).get("date")
        for item in reservoirs
        if isinstance(item.get("latest"), dict) and item["latest"].get("date")
    ]
    date_counts = Counter(latest_dates)
    if requested_date:
        snapshot_date = requested_date
    elif date_counts:
        date_text, _ = max(date_counts.items(), key=lambda pair: (pair[1], pair[0]))
        snapshot_date = datetime.strptime(date_text, "%Y-%m-%d").date()
    else:
        raise RuntimeError("mirror JSON contains no dated observations")

    snapshot_age_days = (datetime.now(tz=timezone.utc).date() - snapshot_date).days
    if not requested_date and snapshot_age_days > MIRROR_MAX_AGE_DAYS:
        raise RuntimeError(
            f"mirror snapshot is stale ({snapshot_age_days} days old; "
            f"maximum is {MIRROR_MAX_AGE_DAYS})"
        )

    metadata = _load_metadata_by_name()
    by_station: dict[str, dict] = {}
    unmatched: list[str] = []
    missing_at_date: list[str] = []
    duplicates: list[str] = []
    history_lookups = 0
    history_lookup_errors: list[dict] = []

    targeted_history_backfill = bool(
        requested_date
        and date_counts[snapshot_date.isoformat()] < MIRROR_MIN_ROWS
    )
    targeted_readings: dict[str, Optional[dict]] = {}
    if targeted_history_backfill:
        candidates = [
            item for item in reservoirs
            if item.get("slug") and metadata.get(_reservoir_key(str(item.get("name") or "")))
        ]
        history_lookups = len(candidates)

        def fetch_history(item: dict) -> tuple[str, Optional[dict], Optional[str]]:
            try:
                return item["slug"], _history_reading(session, item["slug"], snapshot_date), None
            except requests.RequestException as exc:
                return item["slug"], None, str(exc)

        with ThreadPoolExecutor(max_workers=MIRROR_HISTORY_WORKERS) as executor:
            for slug, reading, error in executor.map(fetch_history, candidates):
                targeted_readings[slug] = reading
                if error:
                    history_lookup_errors.append({"slug": slug, "error": error})

    for item in reservoirs:
        name = str(item.get("name") or "").strip()
        meta = metadata.get(_reservoir_key(name))
        if not meta:
            unmatched.append(name)
            continue

        if targeted_history_backfill:
            reading = targeted_readings.get(item.get("slug", ""))
        else:
            reading = item.get("latest") if isinstance(item.get("latest"), dict) else None
            reading_date = reading.get("date") if reading else None
            if reading_date == snapshot_date.isoformat():
                pass
            elif reading_date and reading_date > snapshot_date.isoformat() and item.get("slug"):
                history_lookups += 1
                try:
                    reading = _history_reading(session, item["slug"], snapshot_date)
                except requests.RequestException as exc:
                    history_lookup_errors.append({"slug": item["slug"], "error": str(exc)})
                    reading = None
            else:
                reading = None
        if not reading or reading.get("fill_percentage") is None:
            missing_at_date.append(name)
            continue

        station_id = meta["station_id"]
        if station_id in by_station:
            duplicates.append(name)
            continue
        row = {column: "" for column in TS_COLUMNS}
        row.update(meta)
        row.update({
            "date": snapshot_date.strftime("%Y%m%d"),
            "water_mcm": f"{float(reading['volume_mcm']):g}" if reading.get("volume_mcm") is not None else "",
            "pct_full": f"{float(reading['fill_percentage']):g}",
            "pct_last_year": "",
            "pct_last_week": "",
        })
        by_station[station_id] = row

    rows = sorted(by_station.values(), key=lambda row: row["station_id"])
    if len(rows) < MIRROR_MIN_ROWS:
        raise RuntimeError(
            f"mirror produced only {len(rows)} matched rows for {snapshot_date}; "
            f"minimum is {MIRROR_MIN_ROWS}"
        )

    diagnostics = {
        "mirror_generated": payload.get("generated"),
        "mirror_temporal_coverage": payload.get("temporal_coverage"),
        "mirror_reservoir_count": len(reservoirs),
        "snapshot_age_days": snapshot_age_days,
        "latest_date_counts": dict(sorted(date_counts.items())),
        "matched_rows": len(rows),
        "targeted_history_backfill": targeted_history_backfill,
        "history_lookups": history_lookups,
        "history_lookup_errors": history_lookup_errors,
        "unmatched_reservoirs": sorted(set(unmatched)),
        "missing_at_snapshot_date": sorted(set(missing_at_date)),
        "duplicate_matches": sorted(set(duplicates)),
    }
    return snapshot_date, rows, response.content, diagnostics


def write_snapshot(rows: list[dict], yyyymmdd: str) -> Path:
    """Write one dated snapshot CSV. Old snapshots are kept (archive grows)."""
    TS_DIR.mkdir(parents=True, exist_ok=True)
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


def write_summary(summary: dict) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now(tz=timezone.utc).date().isoformat()
    target = str(summary.get("target_date_override") or "").replace("-", "")
    target_suffix = f"_{target}" if target else ""
    path = LOG_DIR / f"{run_date}{target_suffix}_summary.json"
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    sess = make_session()

    target_date = env_date("DWS_TARGET_DATE")

    summary: dict = {
        "ran_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "target_date_override": target_date.isoformat() if target_date else None,
    }

    found: Optional[tuple[date, bytes, str]] = None
    probes: list[dict] = []
    if target_date:
        yyyymmdd = target_date.strftime("%Y%m%d")
        url = ARCHIVE_TEMPLATES[0].format(d=yyyymmdd)
        pdf_bytes, probe = _probe_pdf(sess, url)
        probes.append(probe)
        if pdf_bytes is not None:
            found = (target_date, pdf_bytes, url)
    else:
        found, probes = find_latest_bulletin(sess)
    summary["official_pdf_probes"] = probes

    source_type: str
    mirror_diagnostics: Optional[dict] = None
    mirror_raw: Optional[bytes] = None
    if found:
        bulletin_date, pdf_bytes, source_url = found
        source_type = "official_pdf"
        yyyymmdd = bulletin_date.strftime("%Y%m%d")
        pdf_path = PDF_DIR / f"Weekly{yyyymmdd}.pdf"
        pdf_path.write_bytes(pdf_bytes)
        print(f"[dws-weekly] using official bulletin {bulletin_date} ({len(pdf_bytes):,} bytes)")
        print(f"[dws-weekly] source: {source_url}")
        rows = parse_pdf(pdf_path, yyyymmdd)
        if not rows:
            print(f"[dws-weekly] FAIL: parsed 0 reservoir rows from {pdf_path.name}")
            summary.update({"status": "parse-empty", "bulletin_date": yyyymmdd})
            write_summary(summary)
            return 1
    else:
        status_counts = Counter(
            str(probe.get("status_code") or probe.get("result")) for probe in probes
        )
        print(
            "[dws-weekly] official PDF unavailable; trying DWS-derived JSON mirror "
            f"(probe results: {dict(status_counts)})"
        )
        try:
            bulletin_date, rows, mirror_raw, mirror_diagnostics = fetch_mirror_snapshot(
                sess, requested_date=target_date
            )
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            print(f"[dws-weekly] FAIL: official PDF and mirror both unavailable: {exc}")
            summary.update({
                "status": "all-sources-failed",
                "mirror_url": MIRROR_URL,
                "mirror_error": str(exc),
            })
            write_summary(summary)
            return 1
        source_type = "dws_derived_mirror"
        source_url = MIRROR_URL
        yyyymmdd = bulletin_date.strftime("%Y%m%d")
        raw_path = RAW_DIR / f"reservoirs_earth_south_africa_{yyyymmdd}.json"
        raw_path.write_bytes(mirror_raw)
        print(f"[dws-weekly] using DWS-derived mirror snapshot {bulletin_date}")
        print(f"[dws-weekly] source: {source_url}")

    snapshot_path_check = TS_DIR / f"southafrica_dws_weekly_{yyyymmdd}.csv"
    if snapshot_path_check.exists():
        print(f"[dws-weekly] {yyyymmdd} already in timeseries/, nothing to do")
        summary.update({
            "status": "already-have",
            "bulletin_date": yyyymmdd,
            "source_type": source_type,
            "source_url": source_url,
            "mirror_diagnostics": mirror_diagnostics,
            "snapshot_path": snapshot_path_check.relative_to(
                OUT_BASE.parent.parent.parent).as_posix(),
        })
        write_summary(summary)
        return 0

    snapshot_path = write_snapshot(rows, yyyymmdd)
    if source_type == "official_pdf":
        write_metadata(rows)

    summary.update({
        "status": "ok",
        "bulletin_date": yyyymmdd,
        "source_type": source_type,
        "source_url": source_url,
        "rows": len(rows),
        "stations": len(rows),
        "mirror_license": MIRROR_LICENSE if source_type == "dws_derived_mirror" else None,
        "mirror_diagnostics": mirror_diagnostics,
        "snapshot_path": snapshot_path.relative_to(OUT_BASE.parent.parent.parent).as_posix(),
    })
    write_summary(summary)
    print(f"[dws-weekly] OK: {len(rows)} reservoirs -> {snapshot_path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

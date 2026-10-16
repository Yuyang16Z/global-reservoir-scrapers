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
shifted by holidays). The GitHub Actions workflow runs on Tuesday
06:00 UTC, comfortably after the publish window, and again on Thursday.

When dws.gov.za refuses (HTTP 403 since 2026-06), bulletins come from the
DWS-derived mirror reservoirs.earth. Its country JSON carries only each dam's
newest reading, so one run takes one bulletin, and when the mirror catches up
several weeks at once the weeks in between exist only on its per-dam history
pages: the 2026-07-27 and 2026-09-21 bulletins were lost that way until dated
re-runs recovered them on 2026-10-15. Every mirror run therefore also lists the
history dates of a few large dams and, when one of them is neither archived nor
decided, reads every dam's history page and backfills it
(catch_up_from_history). Two guards apply to every bulletin taken from the
mirror: at least MIRROR_MIN_ROWS matched dams, and it must not repeat an
archived bulletin (repeated_bulletin) - in June 2026 the mirror dated readings
by its own capture day, so one DWS bulletin appeared under several dates. A dam
the mirror lists twice is read from the entry whose history reproduces the
official bulletin (choose_duplicate_entries).

Output layout (committed to git):
    data/southafrica/dws_weekly/
      timeseries/southafrica_dws_weekly_{YYYYMMDD}.csv   (one file per bulletin)
      metadata/southafrica_dws_reservoirs.csv            (latest official snapshot's reservoirs)
      metadata/mirror_history_dates.csv                  (every mirror history date decided, and why)
      raw/reservoirs_earth_south_africa_{YYYYMMDD}.json  (mirror country JSON, newest-reading runs)
      raw/reservoirs_earth_history_{YYYYMMDD}.csv        (history readings read for that date)
      run_logs/{run_date}[_{target}]_summary.json

Idempotent: a bulletin already in `timeseries/` is never rewritten, and a
history date recorded in mirror_history_dates.csv is not evaluated again.
Re-running on the same day changes nothing but that run's log.

The PDF cache lives at `pdfs/` but is gitignored.

Usage:
    python3 dws_weekly_scraper.py                              # latest published week
    DWS_TARGET_DATE=2026-04-20 python3 dws_weekly_scraper.py   # specific Monday
    DWS_DRY_RUN=1 python3 dws_weekly_scraper.py                # report, write nothing
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import tempfile
import traceback
import unicodedata
from collections import Counter, defaultdict
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
# History dates are listed from the page of the largest dam in each of the
# regions with most dams; a bulletin covers every dam, so any of them shows it.
SENTINEL_REGIONS = 6
# A mirror bulletin repeats an archived one when at least this share of the
# stations both carry has the same percentage, within DUPLICATE_WINDOW_DAYS.
# Consecutive DWS bulletins 2021-2026: median 20%, highest 61% (2023-03-14/20);
# the mirror's re-dated June 2026 copies: 97-99%.
DUPLICATE_SHARE = 0.9
DUPLICATE_WINDOW_DAYS = 21
DUPLICATE_MIN_COMMON = 100
# A dam the mirror lists twice: an entry reproduces an official bulletin when
# one of its readings within this many days carries that bulletin's percentage.
OFFICIAL_MATCH_WINDOW_DAYS = 10
OFFICIAL_MATCH_MIN = 2
EMBEDDED_READING_RE = re.compile(
    r'\{\\"reading_date\\":\\"(\d{4}-\d{2}-\d{2})\\",'
    r'\\"volume_mcm\\":(null|-?\d+(?:\.\d+)?),'
    r'\\"fill_percentage\\":(null|-?\d+(?:\.\d+)?)'
)

OUT_BASE = Path(os.environ.get("OUTPUT_DIR") or
                Path(__file__).resolve().parents[3] / "data" / "southafrica" / "dws_weekly")
PDF_DIR = OUT_BASE / "pdfs"
RAW_DIR = OUT_BASE / "raw"
TS_DIR = OUT_BASE / "timeseries"
META_DIR = OUT_BASE / "metadata"
LOG_DIR = OUT_BASE / "run_logs"
META_PATH = META_DIR / "southafrica_dws_reservoirs.csv"
DECISIONS_PATH = META_DIR / "mirror_history_dates.csv"
DECISION_COLUMNS = ["reading_date", "decision", "dams", "repeats_bulletin",
                    "share_identical", "evidence", "decided_at_utc"]
EVIDENCE_COLUMNS = ["reading_date", "slug", "mirror_name", "station_id",
                    "volume_mcm", "fill_percentage", "used", "page_url", "page_sha256"]

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


def _page_readings(html: str) -> dict[str, dict]:
    """Every reading on one mirror reservoir page: ISO date -> volume, percentage.

    Next.js embeds the underlying readings with decimal precision. Prefer that
    payload over the visible table, which rounds stored volume; the table only
    supplies dates the payload lacks.
    """
    readings: dict[str, dict] = {}
    for day, volume, pct in EMBEDDED_READING_RE.findall(html):
        readings.setdefault(day, {
            "volume_mcm": None if volume == "null" else float(volume),
            "fill_percentage": None if pct == "null" else float(pct),
        })
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        try:
            day = datetime.strptime(cells[0].get_text(" ", strip=True), "%d %b %Y").date()
        except ValueError:
            continue
        pct_text = cells[1].get_text(" ", strip=True).replace("%", "").replace(",", "")
        volume_text = cells[2].get_text(" ", strip=True).replace(",", "")
        readings.setdefault(day.isoformat(), {
            "volume_mcm": _to_float(volume_text),
            "fill_percentage": _to_float(pct_text),
        })
    return readings


class MirrorPages:
    """The mirror's per-dam history pages, each fetched at most once per run."""

    def __init__(self, session: requests.Session):
        self.session = session
        self.readings: dict[str, Optional[dict]] = {}
        self.sha256: dict[str, str] = {}
        self.errors: list[dict] = []

    @staticmethod
    def url(slug: str) -> str:
        return f"{MIRROR_PAGE_ROOT}/{slug}"

    def get(self, slug: str) -> Optional[dict]:
        if slug not in self.readings:
            try:
                response = self.session.get(self.url(slug), timeout=TIMEOUT)
                response.raise_for_status()
            except requests.RequestException as exc:
                self.errors.append({"slug": slug, "error": str(exc)})
                self.readings[slug] = None
            else:
                self.sha256[slug] = hashlib.sha256(response.content).hexdigest()
                self.readings[slug] = _page_readings(response.text)
        return self.readings[slug]

    def get_many(self, slugs) -> None:
        todo = [slug for slug in dict.fromkeys(slugs) if slug and slug not in self.readings]
        if not todo:
            return
        with ThreadPoolExecutor(max_workers=MIRROR_HISTORY_WORKERS) as executor:
            list(executor.map(self.get, todo))


def _snapshot_date(path: Path) -> date:
    return datetime.strptime(path.stem.rsplit("_", 1)[-1], "%Y%m%d").date()


def archived_snapshots() -> dict[date, list[dict]]:
    """Every bulletin already in timeseries/, keyed by its date."""
    out: dict[date, list[dict]] = {}
    for path in sorted(TS_DIR.glob("southafrica_dws_weekly_*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as f:
            out[_snapshot_date(path)] = list(csv.DictReader(f))
    return out


def _percentages(rows: list[dict]) -> dict[str, float]:
    out = {}
    for row in rows:
        pct = _to_float(row.get("pct_full", ""))
        if pct is not None:
            out[row["station_id"]] = pct
    return out


def official_percentages(archived: dict[date, list[dict]]) -> dict[str, list[tuple[date, float]]]:
    """Station -> (bulletin date, % full) from the snapshots parsed from official PDFs.

    The official bulletin prints last year's and last week's percentage; the
    mirror carries neither, so its snapshots leave both columns blank.
    """
    out: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for day, rows in archived.items():
        if not any(row.get("pct_last_year") or row.get("pct_last_week") for row in rows):
            continue
        for station, pct in _percentages(rows).items():
            out[station].append((day, pct))
    return out


def repeated_bulletin(rows: list[dict], snapshot_date: date,
                      archived: dict[date, list[dict]]) -> Optional[dict]:
    """The archived bulletin that `rows` repeats under another date, if any.

    In June 2026 the mirror dated readings by its own capture day: the official
    2026-06-01 bulletin reappears as 06-03, 06-07 and 06-08, and 2026-06-08 as
    06-09 and 06-16 (97-99% identical percentages). Genuine consecutive DWS
    bulletins share at most 61%.
    """
    pct = _percentages(rows)
    best = None
    for day, other_rows in archived.items():
        if day == snapshot_date or abs((day - snapshot_date).days) > DUPLICATE_WINDOW_DAYS:
            continue
        other = _percentages(other_rows)
        common = [station for station in pct if station in other]
        if len(common) < DUPLICATE_MIN_COMMON:
            continue
        share = sum(1 for station in common if pct[station] == other[station]) / len(common)
        if share >= DUPLICATE_SHARE and (best is None or share > best["share"]):
            best = {"bulletin_date": day.strftime("%Y%m%d"), "share": round(share, 3),
                    "common_stations": len(common)}
    return best


def _group_by_station(reservoirs: list[dict], metadata: dict[str, dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for item in reservoirs:
        meta = metadata.get(_reservoir_key(str(item.get("name") or "")))
        if meta:
            groups[meta["station_id"]].append(item)
    return groups


def choose_duplicate_entries(groups: dict[str, list[dict]], pages: MirrorPages,
                             official: dict[str, list[tuple[date, float]]]
                             ) -> tuple[dict[str, str], list[dict]]:
    """Pick one mirror entry for each DWS station the mirror lists more than once.

    The mirror lists Elandsdrift (Q5R001) twice, "Elands Drift Dam" and
    "Elandsdrift Dam" at the same coordinates with different readings, and the
    first entry with a reading used to win. Only "Elandsdrift Dam" reproduces the
    official bulletins (116.2, 117.9 and 125.3 % on the mirror dates holding
    2026-05-25, 06-01 and 06-08); the other repeats 54.9 %. An entry is chosen
    only when it alone reproduces at least OFFICIAL_MATCH_MIN official bulletins;
    otherwise the old rule stands: the first entry, in the mirror's order, that
    has a reading.
    """
    chosen: dict[str, str] = {}
    report: list[dict] = []
    for station, items in sorted(groups.items()):
        if len(items) < 2:
            continue
        pages.get_many(item.get("slug") for item in items)
        scored = []
        for item in items:
            readings = pages.readings.get(item.get("slug") or "") or {}
            reproduced = sum(
                1 for bulletin_day, pct in official.get(station, [])
                if any(reading.get("fill_percentage") == pct
                       and abs((date.fromisoformat(day) - bulletin_day).days)
                       <= OFFICIAL_MATCH_WINDOW_DAYS
                       for day, reading in readings.items())
            )
            scored.append((reproduced, item))
        matching = [item for reproduced, item in scored if reproduced >= OFFICIAL_MATCH_MIN]
        pick = None
        if len(matching) == 1 and all(reproduced == 0 for reproduced, item in scored
                                      if item is not matching[0]):
            pick = matching[0].get("slug")
            chosen[station] = pick
        report.append({
            "station_id": station,
            "chosen_slug": pick,
            "rule": "reproduces_official_bulletins" if pick else "first_entry_with_reading",
            "entries": [{"slug": item.get("slug"), "name": item.get("name"),
                         "readings_count": item.get("readings_count"),
                         "official_bulletins_reproduced": reproduced}
                        for reproduced, item in scored],
        })
    return chosen, report


def _snapshot_row(meta: dict, snapshot_date: date, reading: dict) -> dict:
    row = {column: "" for column in TS_COLUMNS}
    row.update(meta)
    row.update({
        "date": snapshot_date.strftime("%Y%m%d"),
        "water_mcm": f"{float(reading['volume_mcm']):g}" if reading.get("volume_mcm") is not None else "",
        "pct_full": f"{float(reading['fill_percentage']):g}",
        "pct_last_year": "",
        "pct_last_week": "",
    })
    return row


def _number(value: Optional[float]) -> str:
    return "" if value is None else format(value, ".10g")


def _history_rows(reservoirs: list[dict], metadata: dict[str, dict], chosen: dict[str, str],
                  pages: MirrorPages, snapshot_date: date) -> dict:
    """One bulletin built from history pages already read into `pages`."""
    iso = snapshot_date.isoformat()
    by_station: dict[str, dict] = {}
    evidence: list[dict] = []
    unmatched, missing, duplicates = [], [], []
    for item in reservoirs:
        name = str(item.get("name") or "").strip()
        slug = item.get("slug") or ""
        meta = metadata.get(_reservoir_key(name))
        reading = (pages.readings.get(slug) or {}).get(iso) if slug else None
        if not meta:
            unmatched.append(name)
            used = "no_station_match"
        elif meta["station_id"] in chosen and slug != chosen[meta["station_id"]]:
            duplicates.append(name)
            used = "other_entry_chosen"
        elif not reading or reading.get("fill_percentage") is None:
            missing.append(name)
            used = "no_percentage"
        elif meta["station_id"] in by_station:
            duplicates.append(name)
            used = "station_already_taken"
        else:
            by_station[meta["station_id"]] = _snapshot_row(meta, snapshot_date, reading)
            used = "yes"
        if reading:
            evidence.append({
                "reading_date": iso, "slug": slug, "mirror_name": name,
                "station_id": meta["station_id"] if meta else "",
                "volume_mcm": _number(reading.get("volume_mcm")),
                "fill_percentage": _number(reading.get("fill_percentage")),
                "used": used, "page_url": MirrorPages.url(slug),
                "page_sha256": pages.sha256.get(slug, ""),
            })
    return {
        "rows": sorted(by_station.values(), key=lambda row: row["station_id"]),
        "evidence": sorted(evidence, key=lambda row: row["slug"]),
        "unmatched": sorted(set(unmatched)),
        "missing": sorted(set(missing)),
        "duplicates": sorted(set(duplicates)),
    }


def fetch_mirror_snapshot(
    session: requests.Session,
    requested_date: Optional[date] = None,
    pages: Optional[MirrorPages] = None,
) -> tuple[date, list[dict], bytes, dict, dict, Optional[list[dict]]]:
    """One bulletin from the mirror: its newest readings, or `requested_date`.

    Returns (date, rows, raw JSON, diagnostics, payload, evidence); `evidence` is
    the history readings used when the bulletin was built from history pages,
    None when it came from the country JSON's newest readings.
    """
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
    pages = pages if pages is not None else MirrorPages(session)
    errors_before = len(pages.errors)
    chosen, duplicate_entries = choose_duplicate_entries(
        _group_by_station(reservoirs, metadata), pages, official_percentages(archived_snapshots()))
    snapshot_iso = snapshot_date.isoformat()

    targeted_history_backfill = bool(
        requested_date
        and date_counts[snapshot_iso] < MIRROR_MIN_ROWS
    )
    evidence: Optional[list[dict]] = None
    if targeted_history_backfill:
        candidates = [
            item for item in reservoirs
            if item.get("slug") and metadata.get(_reservoir_key(str(item.get("name") or "")))
        ]
        history_lookups = len(candidates)
        pages.get_many(item["slug"] for item in candidates)
        built = _history_rows(reservoirs, metadata, chosen, pages, snapshot_date)
        rows, evidence = built["rows"], built["evidence"]
        unmatched, missing_at_date, duplicates = (built["unmatched"], built["missing"],
                                                  built["duplicates"])
    else:
        by_station: dict[str, dict] = {}
        unmatched, missing_at_date, duplicates = [], [], []
        history_lookups = 0
        for item in reservoirs:
            name = str(item.get("name") or "").strip()
            meta = metadata.get(_reservoir_key(name))
            if not meta:
                unmatched.append(name)
                continue
            station_id = meta["station_id"]
            if station_id in chosen and item.get("slug") != chosen[station_id]:
                duplicates.append(name)
                continue
            reading = item.get("latest") if isinstance(item.get("latest"), dict) else None
            reading_date = reading.get("date") if reading else None
            if reading_date == snapshot_iso:
                pass
            elif reading_date and reading_date > snapshot_iso and item.get("slug"):
                history_lookups += 1
                reading = (pages.get(item["slug"]) or {}).get(snapshot_iso)
            else:
                reading = None
            if not reading or reading.get("fill_percentage") is None:
                missing_at_date.append(name)
                continue
            if station_id in by_station:
                duplicates.append(name)
                continue
            by_station[station_id] = _snapshot_row(meta, snapshot_date, reading)
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
        "history_lookup_errors": pages.errors[errors_before:],
        "unmatched_reservoirs": sorted(set(unmatched)),
        "missing_at_snapshot_date": sorted(set(missing_at_date)),
        "duplicate_matches": sorted(set(duplicates)),
        "duplicate_entries": duplicate_entries,
    }
    return snapshot_date, rows, response.content, diagnostics, payload, evidence


def _sentinel_slugs(reservoirs: list[dict]) -> list[str]:
    """The largest dam of each of the SENTINEL_REGIONS regions with most dams."""
    by_region: dict[str, list[dict]] = defaultdict(list)
    for item in reservoirs:
        if item.get("slug") and item.get("readings_count"):
            by_region[str(item.get("region") or "")].append(item)
    regions = sorted(by_region, key=lambda region: (-len(by_region[region]), region))
    return [
        max(by_region[region],
            key=lambda item: (float(item.get("capacity_total_mcm") or 0), item["slug"]))["slug"]
        for region in regions[:SENTINEL_REGIONS]
    ]


def load_decisions() -> dict[str, dict]:
    if not DECISIONS_PATH.exists():
        return {}
    with DECISIONS_PATH.open(encoding="utf-8-sig", newline="") as f:
        return {row["reading_date"]: row for row in csv.DictReader(f)}


def record_decision(decision: dict) -> None:
    rows = load_decisions()
    rows[decision["reading_date"]] = {column: decision.get(column, "") for column in DECISION_COLUMNS}
    META_DIR.mkdir(parents=True, exist_ok=True)
    with DECISIONS_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_COLUMNS)
        writer.writeheader()
        writer.writerows(rows[day] for day in sorted(rows))


def write_history_evidence(snapshot_date: date, evidence: list[dict]) -> Path:
    """The history readings one bulletin was built from, with each page's SHA-256."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"reservoirs_earth_history_{snapshot_date:%Y%m%d}.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EVIDENCE_COLUMNS)
        writer.writeheader()
        writer.writerows(evidence)
    return path


def _repo_path(path: Path) -> str:
    return path.relative_to(OUT_BASE.parent.parent.parent).as_posix()


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def catch_up_from_history(payload: dict, pages: MirrorPages, dry_run: bool = False) -> dict:
    """Backfill bulletin dates the mirror's history holds but the archive lacks.

    The page of the largest dam in each of the main regions lists the history
    dates. A date neither in timeseries/ nor in mirror_history_dates.csv is a
    candidate; one pass over every dam's page then builds each candidate, which
    is written only when it has MIRROR_MIN_ROWS dams and repeats no archived
    bulletin. Every decision is recorded, so a date is evaluated once. Nothing is
    decided when any history page failed: the next run tries again.
    """
    reservoirs = [item for item in payload.get("reservoirs", []) if isinstance(item, dict)]
    metadata = _load_metadata_by_name()
    archived = archived_snapshots()
    decided = load_decisions()
    sentinels = _sentinel_slugs(reservoirs)
    for slug in sentinels:
        pages.get(slug)
    seen = sorted({day for slug in sentinels for day in (pages.readings.get(slug) or {})})
    archived_iso = {day.isoformat() for day in archived}
    candidates = [day for day in seen if day not in archived_iso and day not in decided]
    report: dict = {"sentinels": sentinels, "sentinel_dates": seen, "candidates": candidates,
                    "decisions": []}
    if not candidates:
        return report

    matched = [item for item in reservoirs
               if item.get("slug") and metadata.get(_reservoir_key(str(item.get("name") or "")))]
    errors_before = len(pages.errors)
    pages.get_many(item["slug"] for item in matched)
    chosen, duplicate_entries = choose_duplicate_entries(
        _group_by_station(reservoirs, metadata), pages, official_percentages(archived))
    report["history_pages_read"] = len(matched)
    report["duplicate_entries"] = duplicate_entries
    report["history_lookup_errors"] = pages.errors[errors_before:]
    if report["history_lookup_errors"]:
        report["status"] = "incomplete_history_pass"
        return report

    for iso in candidates:
        day = date.fromisoformat(iso)
        built = _history_rows(reservoirs, metadata, chosen, pages, day)
        rows = built["rows"]
        decision = {"reading_date": iso, "dams": len(rows), "decided_at_utc": _now_utc(),
                    "missing_at_date": len(built["missing"])}
        repeat = repeated_bulletin(rows, day, archived) if len(rows) >= MIRROR_MIN_ROWS else None
        if len(rows) < MIRROR_MIN_ROWS:
            decision["decision"] = "rejected_too_few_dams"
        elif repeat:
            decision.update(decision="rejected_repeats_bulletin",
                            repeats_bulletin=repeat["bulletin_date"],
                            share_identical=repeat["share"])
        else:
            decision["decision"] = "backfilled"
            archived[day] = rows
        if not dry_run:
            if built["evidence"]:
                decision["evidence"] = _repo_path(write_history_evidence(day, built["evidence"]))
            if decision["decision"] == "backfilled":
                decision["snapshot_path"] = _repo_path(write_snapshot(rows, f"{day:%Y%m%d}"))
            record_decision(decision)
        print(f"[dws-weekly] history {iso}: {decision['decision']} ({len(rows)} dams"
              + (f", repeats {repeat['bulletin_date']} at {repeat['share']:.0%}" if repeat else "")
              + ")")
        report["decisions"].append(decision)
    return report


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


def finish(summary: dict, dry_run: bool) -> None:
    if dry_run:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        write_summary(summary)


def main() -> int:
    dry_run = os.environ.get("DWS_DRY_RUN", "").strip() not in ("", "0")
    if not dry_run:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        PDF_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DIR.mkdir(parents=True, exist_ok=True)

    sess = make_session()

    target_date = env_date("DWS_TARGET_DATE")

    summary: dict = {
        "ran_at_utc": _now_utc(),
        "target_date_override": target_date.isoformat() if target_date else None,
    }
    if dry_run:
        summary["dry_run"] = True

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
    payload: Optional[dict] = None
    evidence: Optional[list[dict]] = None
    pages: Optional[MirrorPages] = None
    if found:
        bulletin_date, pdf_bytes, source_url = found
        source_type = "official_pdf"
        yyyymmdd = bulletin_date.strftime("%Y%m%d")
        pdf_dir = Path(tempfile.mkdtemp()) if dry_run else PDF_DIR
        pdf_path = pdf_dir / f"Weekly{yyyymmdd}.pdf"
        pdf_path.write_bytes(pdf_bytes)
        print(f"[dws-weekly] using official bulletin {bulletin_date} ({len(pdf_bytes):,} bytes)")
        print(f"[dws-weekly] source: {source_url}")
        rows = parse_pdf(pdf_path, yyyymmdd)
        if not rows:
            print(f"[dws-weekly] FAIL: parsed 0 reservoir rows from {pdf_path.name}")
            summary.update({"status": "parse-empty", "bulletin_date": yyyymmdd})
            finish(summary, dry_run)
            return 1
    else:
        status_counts = Counter(
            str(probe.get("status_code") or probe.get("result")) for probe in probes
        )
        print(
            "[dws-weekly] official PDF unavailable; trying DWS-derived JSON mirror "
            f"(probe results: {dict(status_counts)})"
        )
        pages = MirrorPages(sess)
        try:
            (bulletin_date, rows, mirror_raw, mirror_diagnostics,
             payload, evidence) = fetch_mirror_snapshot(sess, requested_date=target_date,
                                                        pages=pages)
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            # Both routes are out: the official pages answer 403 and the mirror
            # cannot advance past its own upstream. Refusing to write stale data
            # is correct, but that is a SOURCE outage, not a scraper fault, so
            # the run records it and exits clean. scripts/monitor_source_freshness.py
            # is what raises the alarm once the archived observation falls behind
            # the publication cadence, and it already reports this source.
            print(
                "[dws-weekly] source unavailable: official PDF and mirror both "
                f"unusable ({exc}). No data written; freshness monitor will flag "
                "the gap."
            )
            summary.update({
                "status": "source_unavailable",
                "mirror_url": MIRROR_URL,
                "mirror_error": str(exc),
            })
            finish(summary, dry_run)
            return 0
        source_type = "dws_derived_mirror"
        source_url = MIRROR_URL
        yyyymmdd = bulletin_date.strftime("%Y%m%d")
        if not dry_run:
            if evidence is None:
                raw_path = RAW_DIR / f"reservoirs_earth_south_africa_{yyyymmdd}.json"
                raw_path.write_bytes(mirror_raw)
            else:
                write_history_evidence(bulletin_date, evidence)
        print(f"[dws-weekly] using DWS-derived mirror snapshot {bulletin_date}")
        print(f"[dws-weekly] source: {source_url}")

    snapshot_path = TS_DIR / f"southafrica_dws_weekly_{yyyymmdd}.csv"
    repeat = None
    if snapshot_path.exists():
        status = "already-have"
        print(f"[dws-weekly] {yyyymmdd} already in timeseries/, nothing to do")
    elif source_type == "dws_derived_mirror" and (
            repeat := repeated_bulletin(rows, bulletin_date, archived_snapshots())):
        status = "repeats_archived_bulletin"
        print(f"[dws-weekly] {yyyymmdd} repeats the archived {repeat['bulletin_date']} "
              f"bulletin ({repeat['share']:.0%} identical); not written")
    else:
        status = "ok"
        if not dry_run:
            snapshot_path = write_snapshot(rows, yyyymmdd)
            if source_type == "official_pdf":
                write_metadata(rows)
        print(f"[dws-weekly] OK: {len(rows)} reservoirs -> {snapshot_path.name}")

    summary.update({
        "status": status,
        "bulletin_date": yyyymmdd,
        "source_type": source_type,
        "source_url": source_url,
    })
    if status == "ok":
        summary.update({
            "rows": len(rows),
            "stations": len(rows),
            "mirror_license": MIRROR_LICENSE if source_type == "dws_derived_mirror" else None,
        })
    if repeat:
        summary["repeats_bulletin"] = repeat
    summary["mirror_diagnostics"] = mirror_diagnostics
    if status != "repeats_archived_bulletin":
        summary["snapshot_path"] = _repo_path(snapshot_path)
    if target_date and evidence is not None and status != "already-have" and not dry_run:
        record_decision({
            "reading_date": bulletin_date.isoformat(),
            "decision": "backfilled" if status == "ok" else "rejected_repeats_bulletin",
            "dams": len(rows),
            "repeats_bulletin": repeat["bulletin_date"] if repeat else "",
            "share_identical": repeat["share"] if repeat else "",
            "evidence": _repo_path(RAW_DIR / f"reservoirs_earth_history_{yyyymmdd}.csv"),
            "decided_at_utc": _now_utc(),
        })

    if source_type == "dws_derived_mirror" and not target_date:
        # A fault here must not cost the bulletin just captured: the workflow
        # commits only after the scraper exits 0.
        try:
            summary["history_catch_up"] = catch_up_from_history(payload, pages, dry_run)
        except Exception as exc:  # noqa: BLE001 - reported in the run log
            traceback.print_exc()
            summary["history_catch_up"] = {"status": "error", "error": repr(exc)}
    finish(summary, dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())

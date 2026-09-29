"""CAMMESA - daily hydro snapshot "Datos de Embalses y Centrales Hidráulicas".

Source (one static page per Argentina calendar day):
  https://microfe.cammesa.com/static-content/CammesaWeb/download-manager-files/
      unifilares/hidro/<YYYYMMDD>/www.cammesa.com/OpenDocument.html

Each page reports, per reservoir box: Cota Hoy (level, msnm), Cota Max / Cota Min
(operating band), Turbinado and Vertido (m3/s), plus pumping (MWh) for Río Grande,
and a set of river-flow boxes. Boxes seen 2026-08-28..2026-09-28: Planicie
Banderita (Los Barreales), Alicurá, Mari Menuco, El Chocón, Piedra del Águila,
Arroyito, P.P. Leufú, Yacyretá, Salto Grande, Ameghino, Futaleufú, Río Grande,
Los Caracoles, Punta Negra, Ullum (turbined only), Quebrada de Ullum.

Retention: ROLLING WINDOW of about 32 days. On 2026-09-28 the earliest page was
2026-08-28; 2026-08-27 had been online that morning and was gone by the evening;
2026-08-31 was never published; the Internet Archive holds no copies. This is the
only current public source found for the Comahue reservoirs after AIC stopped its
monthly operation reports in February 2021.

Design:
- Every run re-requests every date from (Argentina today - WINDOW_DAYS) to today,
  so a missed or delayed run loses nothing while the window lasts.
- The page's CDN (F5) injects <script id="f5_cspm"> with a random token into every
  response, so raw bytes differ on every request even when the data do not
  (verified 2026-09-28 on 31 pages). A page is stored once per *cleaned* content
  hash; a genuine data change for a date is stored as a new version, never
  overwritten.
- CSVs are rebuilt from the stored raw pages on every run, so reruns are
  idempotent and a parser fix can be replayed over the whole archive.
- The page prints its own date ("SITUACION DE CUENCAS HIDRAULICAS DEL MM/DD/YYYY");
  that date, not the fetch date, is `page_date`. For Salto Grande the level on page
  date D equals the operator's (CTM) 24:00 reading of D-1 on 30 of 30 checked days,
  i.e. the level is the value at 00:00 of the page date.

Licence: CAMMESA's site terms prohibit reproduction, distribution or transmission
without CAMMESA's express authorisation (reuse_status `prohibited`, checked
2026-09-28). Collected here by owner decision of 2026-09-28; see
DATA_LICENCE_NOTICE.md. Presence in this repository is not permission to republish.

Environment:
  OUTPUT_DIR      output root (default <script_dir>/outputs)
  START_DATE      optional backfill start (YYYY-MM-DD); overrides WINDOW_DAYS
  END_DATE        optional end (YYYY-MM-DD); default Argentina today
  WINDOW_DAYS     look-back length in days (default 40)

Outputs (under OUTPUT_DIR):
  raw/<YYYYMMDD>__<content12>.html        page bytes as first served
  timeseries/argentina_cammesa_hidro_reservoirs_all_versions.csv
  timeseries/argentina_cammesa_hidro_reservoirs.csv       latest version per page date
  timeseries/argentina_cammesa_hidro_rivers.csv           river-flow boxes, latest version
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import hashlib
import html as html_mod
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

URL_TEMPLATE = ("https://microfe.cammesa.com/static-content/CammesaWeb/download-manager-files/"
                "unifilares/hidro/{ymd}/www.cammesa.com/OpenDocument.html")
SOURCE_AGENCY = "CAMMESA (Compañía Administradora del Mercado Mayorista Eléctrico S.A.)"
ART = timezone(timedelta(hours=-3))  # Argentina, no daylight saving
TIMEOUT = 60
REQUEST_BACKOFFS = (5, 20, 60)
PAUSE_SECONDS = 1.5
HEADERS = {"User-Agent": ("Mozilla/5.0 (compatible; global-reservoir-scrapers/1.0; "
                          "+https://github.com/Yuyang16Z/global-reservoir-scrapers)")}

F5_TOKEN = re.compile(rb'<script id="f5_cspm">.*?</script>', re.S)
PAGE_DATE_RE = re.compile(r"SITUACION DE CUENCAS HIDRAULICAS DEL\s+(\d{2})/(\d{2})/(\d{4})")
TABLE_RE = re.compile(r"<table\b([^>]*)>(.*?)</table>", re.S | re.I)
ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", re.S | re.I)
POS_RE = re.compile(r"left:(\d+);top:(\d+)")
NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")

FIELD_MAP = {
    "cota hoy": "level_today_masl",
    "cota max": "level_max_masl",
    "cota min": "level_min_masl",
    "turbinado": "turbined_m3s",
    "vertido": "spilled_m3s",
    "bombeo": "pumping_mwh",
}
RES_COLUMNS = ["page_date", "box_name", "box_subname", "level_today_masl", "level_max_masl",
               "level_min_masl", "turbined_m3s", "spilled_m3s", "pumping_mwh", "unparsed_fields",
               "version", "n_versions", "first_seen_utc", "raw_file", "content_sha256"]
RIV_COLUMNS = ["page_date", "box_name", "flow_m3s", "box_left", "box_top",
               "first_seen_utc", "raw_file", "content_sha256"]
FETCH_LOG_COLUMNS = ["requested_date", "http_status", "bytes", "content_sha256",
                     "fetched_at_utc", "stored_as"]


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(body: bytes) -> str:
    """sha256 of the page without the CDN's per-response monitoring script."""
    return hashlib.sha256(F5_TOKEN.sub(b"", body)).hexdigest()


def fetch(url: str) -> tuple[int, bytes, str]:
    """GET with bounded retries on network errors. HTTP 404 means 'not (or no longer) published'."""
    last = ""
    for attempt in range(len(REQUEST_BACKOFFS) + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code in (200, 404):
                return r.status_code, (r.content if r.status_code == 200 else b""), ""
            last = f"HTTP {r.status_code}"
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
        if attempt < len(REQUEST_BACKOFFS):
            time.sleep(REQUEST_BACKOFFS[attempt])
    return 0, b"", last


def number(text: str) -> float | None:
    m = NUM_RE.search(text.replace(",", ""))
    return float(m.group(0)) if m else None


def cell_texts(row_html: str) -> list[str]:
    return [re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", "", c))).strip()
            for c in CELL_RE.findall(row_html)]


def parse_page(body: bytes) -> tuple[str | None, list[dict], list[dict]]:
    """Return (page_date ISO, reservoir rows, river rows)."""
    text = body.decode("latin-1")
    m = PAGE_DATE_RE.search(text)
    page_date = f"{m.group(3)}-{m.group(1)}-{m.group(2)}" if m else None  # page prints MM/DD/YYYY
    reservoirs: list[dict] = []
    rivers: list[dict] = []
    for attrs, inner in TABLE_RE.findall(text):
        pos = POS_RE.search(attrs)
        if not pos:
            continue  # reference tables at the bottom of the page are not daily observations
        rows = [cell_texts(r) for r in ROW_RE.findall(inner)]
        names = [c[0] for c in rows if len(c) == 1 and c[0] and not re.search(r"\d", c[0])]
        pairs = [c for c in rows if len(c) == 2 and c[0].endswith(":")]
        if pairs:
            rec = {k: None for k in RES_COLUMNS}
            rec.update(page_date=page_date, box_name=names[0] if names else "",
                       box_subname=names[1] if len(names) > 1 else "")
            leftovers = []
            for label, value in pairs:
                key = FIELD_MAP.get(label.rstrip(":").strip().lower())
                if key:
                    rec[key] = number(value)
                else:
                    leftovers.append(f"{label} {value}")
            rec["unparsed_fields"] = "; ".join(leftovers)
            reservoirs.append(rec)
        else:
            flows = [number(x) for c in rows for x in c if "m3/s" in x]
            if flows:
                rivers.append(dict(page_date=page_date, box_name=names[0] if names else "",
                                   flow_m3s=flows[0], box_left=int(pos.group(1)),
                                   box_top=int(pos.group(2))))
    return page_date, reservoirs, rivers


def capture(d0: date, d1: date, summary: dict) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fetch_log = []
    d = d0
    while d <= d1:
        status, body, err = fetch(URL_TEMPLATE.format(ymd=d.strftime("%Y%m%d")))
        chash = content_hash(body)[:12] if body else ""
        stored = ""
        if status == 200 and body:
            summary["http_200"] += 1
            name = f"{d:%Y%m%d}__{chash}.html"
            if not (RAW_DIR / name).exists():
                (RAW_DIR / name).write_bytes(body)
                summary["new_raw_pages"].append(name)
                stored = name
            else:
                stored = "unchanged"
        elif status == 404:
            summary["http_404"] += 1
        else:
            summary["errors"].append(f"{d.isoformat()}: {err}")
        fetch_log.append(dict(requested_date=d.isoformat(), http_status=status, bytes=len(body),
                              content_sha256=chash, fetched_at_utc=utc_now(), stored_as=stored or err))
        d += timedelta(days=1)
        time.sleep(PAUSE_SECONDS)
    summary["fetch_log"] = fetch_log


def first_seen_times() -> dict[str, str]:
    """raw file -> earliest fetched_at_utc recorded in any run summary (falls back to mtime)."""
    seen: dict[str, str] = {}
    if RUN_LOG_DIR.is_dir():
        for p in sorted(RUN_LOG_DIR.glob("*_summary.json")):
            try:
                for row in json.loads(p.read_text(encoding="utf-8")).get("fetch_log", []):
                    name = row.get("stored_as", "")
                    if name.endswith(".html"):
                        seen.setdefault(name, row["fetched_at_utc"])
            except (ValueError, KeyError):
                continue
    for p in RAW_DIR.glob("*.html"):
        seen.setdefault(p.name, datetime.fromtimestamp(p.stat().st_mtime, timezone.utc)
                        .isoformat(timespec="seconds"))
    return seen


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def rebuild(summary: dict) -> None:
    seen = first_seen_times()
    by_day: dict[str, list[Path]] = {}
    for p in RAW_DIR.glob("*.html"):
        by_day.setdefault(p.name[:8], []).append(p)
    all_rows, latest_rows, river_rows, mismatched = [], [], [], []
    for day in sorted(by_day):
        versions = sorted(by_day[day], key=lambda p: (seen.get(p.name, ""), p.name))
        for i, p in enumerate(versions, start=1):
            body = p.read_bytes()
            chash = content_hash(body)
            page_date, res, riv = parse_page(body)
            if page_date and page_date.replace("-", "") != day:
                mismatched.append(p.name)
            for r in res:
                r.update(version=i, n_versions=len(versions), first_seen_utc=seen.get(p.name, ""),
                         raw_file=p.name, content_sha256=chash)
            all_rows += res
            if i == len(versions):
                latest_rows += res
                for r in riv:
                    r.update(first_seen_utc=seen.get(p.name, ""), raw_file=p.name, content_sha256=chash)
                river_rows += riv
    write_csv(TS_DIR / "argentina_cammesa_hidro_reservoirs_all_versions.csv", RES_COLUMNS, all_rows)
    write_csv(TS_DIR / "argentina_cammesa_hidro_reservoirs.csv", RES_COLUMNS, latest_rows)
    write_csv(TS_DIR / "argentina_cammesa_hidro_rivers.csv", RIV_COLUMNS, river_rows)
    dates = sorted({r["page_date"] for r in latest_rows if r["page_date"]})
    summary.update(raw_pages=sum(len(v) for v in by_day.values()), page_dates=len(dates),
                   earliest_page_date=dates[0] if dates else None,
                   latest_page_date=dates[-1] if dates else None,
                   reservoir_rows=len(latest_rows), river_rows=len(river_rows),
                   page_date_mismatches=mismatched)


def main() -> int:
    started = utc_now()
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    today_art = datetime.now(ART).date()
    end = date.fromisoformat(os.environ["END_DATE"]) if os.environ.get("END_DATE") else today_art
    if os.environ.get("START_DATE"):
        start = date.fromisoformat(os.environ["START_DATE"])
    else:
        start = end - timedelta(days=int(os.environ.get("WINDOW_DAYS") or 40))
    summary = {"source": "argentina/cammesa_hidro", "agency": SOURCE_AGENCY, "started_utc": started,
               "requested_range": [start.isoformat(), end.isoformat()], "http_200": 0, "http_404": 0,
               "new_raw_pages": [], "errors": []}
    log(f"CAMMESA hidro: requesting {start} .. {end}")
    capture(start, end, summary)
    rebuild(summary)
    summary["finished_utc"] = utc_now()
    if summary["http_200"] == 0:
        summary["status"] = "failed"
    elif summary["errors"] or summary["page_date_mismatches"]:
        summary["status"] = "partial"
    else:
        summary["status"] = "ok"
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    log(json.dumps({k: v for k, v in summary.items() if k != "fetch_log"}, ensure_ascii=False))
    if summary["http_200"] == 0:
        log("ERROR: no page could be retrieved in this run (source down, URL changed or blocked).")
        return 1
    if summary["page_date_mismatches"]:
        log(f"WARNING: page date differs from URL date in {summary['page_date_mismatches']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

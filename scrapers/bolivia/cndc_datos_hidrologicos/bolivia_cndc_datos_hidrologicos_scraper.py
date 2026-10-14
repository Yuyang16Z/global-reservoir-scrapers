"""CNDC Bolivia - daily hydrological data of the hydro reservoirs ("Datos hidrologicos").

Source (the JSON behind the daily hydrological table of CNDC's website):
  https://www.cndc.bo/wp-json/cndc/v1/datos-hidrologicos?fecha=<YYYY-MM-DD>

CNDC (Comite Nacional de Despacho de Carga) is Bolivia's power-system operator. For a
date (`fecha`) the response lists, per hydro system (Zongo, Taquesi, Corani, Misicuni,
Miguilla, Kanata, San Jacinto on 2026-10-14), each reservoir or pond (29 on that day)
with `caudal_m3s` (flow), `capacidad_hm3` (capacity), `vol_inicial_hm3` and
`vol_final_hm3` (volume at the start and at the end of the day).

Retention: ROLLING WINDOW of 30 dates. Every response lists `fechas_disponibles`: on
2026-10-14 the dates 2026-09-15 to 2026-10-14; an older date returns
{"sin_datos": true}. The current date is served shortly after midnight Bolivia time
(seen at 06:41 UTC on 2026-10-14). CNDC's weekly statistics files (2002 onwards) keep
weekly storage and inflow, but these daily values exist nowhere else.

Not stated by CNDC, noted on 2026-10-14: a date's end-of-day volume differs from the
next date's start-of-day volume (Corani: 2026-10-12 end 53.67 hm3, 2026-10-13 start
53.83 hm3), and the current date's end-of-day volume is served before the day has
passed, so end-of-day volumes look like programmed values. Every version of a date is
kept so that later revisions can be told apart from the first publication.

Design:
- Every run reads the list of available dates and requests each of them, PAUSE_SECONDS
  apart, so a missed or delayed run loses nothing while the window lasts.
- `fechas_disponibles` changes every day, so a response is stored once per hash of its
  data part (`fecha` and `sistemas`); a genuine change for a date is stored as a new
  version, never overwritten.
- CSVs are rebuilt from the stored responses on every run, so reruns are idempotent
  and a parser fix can be replayed over the whole archive.
- HTTP 403/412/429 stops the run (the project does not work around blocks).

Licence: the website footer reads "(c) 2026 CNDC - Comite Nacional de Despacho de Carga
de Bolivia. Todos los derechos reservados." and grants no reuse permission
(reuse_status `restricted_use`, checked 2026-10-14). Collected here by owner decision
of 2026-10-14; see DATA_LICENCE_NOTICE.md. Presence in this repository is not
permission to republish.

Environment:
  OUTPUT_DIR      output root (default <script_dir>/outputs)
  PAUSE_SECONDS   pause between requests (default 2)

Outputs (under OUTPUT_DIR):
  raw/<YYYY>/<YYYYMMDD>__<data12>.json        response bytes as first served
  timeseries/bolivia_cndc_datos_hidrologicos_all_versions.csv
  timeseries/bolivia_cndc_datos_hidrologicos.csv   latest version per record date
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

ENDPOINT = "https://www.cndc.bo/wp-json/cndc/v1/datos-hidrologicos"
SOURCE_AGENCY = "CNDC (Comite Nacional de Despacho de Carga), Bolivia"
TIMEOUT = 60
REQUEST_BACKOFFS = (5, 20, 60)
PAUSE_SECONDS = float(os.environ.get("PAUSE_SECONDS") or 2)
BLOCKED = (403, 412, 429)
HEADERS = {"User-Agent": ("Mozilla/5.0 (compatible; global-reservoir-scrapers/1.0; "
                          "+https://github.com/Yuyang16Z/global-reservoir-scrapers)"),
           "Accept": "application/json"}

FIELDS = ["caudal_m3s", "capacidad_hm3", "vol_inicial_hm3", "vol_final_hm3"]
COLUMNS = ["record_date", "system", "reservoir", *FIELDS, "unparsed_fields", "version", "n_versions",
           "first_seen_utc", "raw_file", "data_sha256"]
FETCH_LOG_COLUMNS = ["requested_date", "http_status", "bytes", "data_sha256", "fetched_at_utc", "stored_as"]
TS_NAME = "bolivia_cndc_datos_hidrologicos"


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def data_hash(doc: dict) -> str:
    """sha256 of the data part of a response; the rolling list of available dates is left out."""
    part = {"fecha": doc.get("fecha"), "sistemas": doc.get("sistemas")}
    return hashlib.sha256(json.dumps(part, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def fetch(url: str) -> tuple[int, bytes, str]:
    """GET with bounded retries on network errors and server errors; a block is returned at once."""
    last = ""
    for attempt in range(len(REQUEST_BACKOFFS) + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200 or r.status_code in BLOCKED or 400 <= r.status_code < 500:
                return r.status_code, (r.content if r.status_code == 200 else b""), (
                    "" if r.status_code == 200 else f"HTTP {r.status_code}")
            last = f"HTTP {r.status_code}"
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
        if attempt < len(REQUEST_BACKOFFS):
            time.sleep(REQUEST_BACKOFFS[attempt])
    return 0, b"", last


def parse(doc: dict) -> list[dict]:
    """One row per (system, reservoir) of a response."""
    rows = []
    for system in doc.get("sistemas") or []:
        for res in system.get("embalses") or []:
            extra = {k: v for k, v in res.items() if k not in FIELDS and k != "embalse"}
            rows.append({"record_date": doc.get("fecha"), "system": system.get("sistema", ""),
                         "reservoir": res.get("embalse", ""),
                         **{f: ("" if res.get(f) is None else res.get(f)) for f in FIELDS},
                         "unparsed_fields": json.dumps(extra, ensure_ascii=False, sort_keys=True) if extra else ""})
    return rows


def capture(summary: dict) -> None:
    """Request every available date once and store responses whose data are new."""
    status, body, err = fetch(ENDPOINT)
    if status != 200:
        summary["errors"].append(f"date list: {err}")
        summary["blocked"] = status in BLOCKED
        return
    try:
        dates = sorted(set(json.loads(body).get("fechas_disponibles") or []))
    except ValueError:
        summary["errors"].append("date list: response is not JSON")
        return
    summary["available_dates"] = dates
    fetch_log = []
    for i, day in enumerate(dates):
        if i:
            time.sleep(PAUSE_SECONDS)
        status, body, err = fetch(f"{ENDPOINT}?fecha={day}")
        stored, dhash = "", ""
        if status == 200:
            try:
                doc = json.loads(body)
            except ValueError:
                doc, err = None, "response is not JSON"
            if doc is not None and doc.get("sin_datos"):
                summary["sin_datos"].append(day)
                stored = "sin_datos"
            elif doc is not None:
                summary["http_200"] += 1
                if doc.get("fecha") != day:
                    summary["date_mismatches"].append(f"{day}: response fecha {doc.get('fecha')}")
                dhash = data_hash(doc)
                name = f"{day.replace('-', '')}__{dhash[:12]}.json"
                path = RAW_DIR / day[:4] / name
                if path.exists():
                    stored = "unchanged"
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(body)
                    summary["new_raw_files"].append(name)
                    stored = name
            else:
                summary["errors"].append(f"{day}: {err}")
        else:
            summary["errors"].append(f"{day}: {err}")
        fetch_log.append(dict(requested_date=day, http_status=status, bytes=len(body), data_sha256=dhash[:12],
                              fetched_at_utc=utc_now(), stored_as=stored or err))
        if status in BLOCKED:
            summary["blocked"] = True
            log(f"HTTP {status} for {day}: stopping (project scraping policy)")
            break
    summary["fetch_log"] = fetch_log


def first_seen_times(current_log: list[dict]) -> dict[str, str]:
    """raw file -> earliest fetched_at_utc recorded in a run summary or in this run's fetch log (falls back
    to the file's mtime). This run's own summary is written after the rebuild, so its log is passed in;
    otherwise a new file's time would change between this run and the next one."""
    seen: dict[str, str] = {}
    logs = []
    if RUN_LOG_DIR.is_dir():
        for p in sorted(RUN_LOG_DIR.glob("*_summary.json")):
            try:
                logs.append(json.loads(p.read_text(encoding="utf-8")).get("fetch_log", []))
            except ValueError:
                continue
    for rows in [*logs, current_log]:
        for row in rows:
            name = row.get("stored_as", "")
            if name.endswith(".json") and row.get("fetched_at_utc"):
                seen.setdefault(name, row["fetched_at_utc"])
    for p in RAW_DIR.rglob("*.json"):
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
    seen = first_seen_times(summary.get("fetch_log", []))
    by_day: dict[str, list[Path]] = {}
    for p in RAW_DIR.rglob("*.json"):
        by_day.setdefault(p.name[:8], []).append(p)
    all_rows, latest_rows, mismatched = [], [], []
    for day in sorted(by_day):
        # first seen, then the file's own write time (two versions never come from one run)
        versions = sorted(by_day[day], key=lambda p: (seen.get(p.name, ""), p.stat().st_mtime_ns, p.name))
        for i, p in enumerate(versions, start=1):
            doc = json.loads(p.read_bytes())
            if (doc.get("fecha") or "").replace("-", "") != day:
                mismatched.append(p.name)
            rows = parse(doc)
            for r in rows:
                r.update(version=i, n_versions=len(versions), first_seen_utc=seen.get(p.name, ""),
                         raw_file=f"{day[:4]}/{p.name}", data_sha256=data_hash(doc))
            all_rows += rows
            if i == len(versions):
                latest_rows += rows
    write_csv(TS_DIR / f"{TS_NAME}_all_versions.csv", COLUMNS, all_rows)
    write_csv(TS_DIR / f"{TS_NAME}.csv", COLUMNS, latest_rows)
    dates = sorted({r["record_date"] for r in latest_rows if r["record_date"]})
    summary.update(raw_files=sum(len(v) for v in by_day.values()), record_dates=len(dates),
                   earliest_record_date=dates[0] if dates else None,
                   latest_record_date=dates[-1] if dates else None,
                   rows=len(latest_rows), reservoirs=len({(r["system"], r["reservoir"]) for r in latest_rows}),
                   dates_with_revisions=sorted(d for d, v in by_day.items() if len(v) > 1),
                   stored_date_mismatches=mismatched)


def main() -> int:
    started = utc_now()
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    summary = {"source": "bolivia/cndc_datos_hidrologicos", "agency": SOURCE_AGENCY, "started_utc": started,
               "endpoint": ENDPOINT, "http_200": 0, "new_raw_files": [], "sin_datos": [], "date_mismatches": [],
               "errors": [], "blocked": False}
    log("CNDC datos hidrologicos: reading the available dates")
    capture(summary)
    rebuild(summary)
    summary["finished_utc"] = utc_now()
    if summary["http_200"] == 0 or summary["blocked"]:
        summary["status"] = "failed"
    elif summary["errors"] or summary["date_mismatches"] or summary["stored_date_mismatches"]:
        summary["status"] = "partial"
    else:
        summary["status"] = "ok"
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    log(json.dumps({k: v for k, v in summary.items() if k not in ("fetch_log", "available_dates")},
                   ensure_ascii=False))
    if summary["status"] == "failed":
        log("ERROR: no date could be retrieved in this run (source down, URL changed or blocked).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""ICE / CENCE (Costa Rica) - reservoir levels on the "Boletin Intra-Diario" page.

Source:
  https://apps.grupoice.com/CenceWeb/CenceBoletinIntraDiario.jsf?init=true

The page of the Centro Nacional de Control de Energia (CENCE, operated by ICE's DOCSE) carries, in its panel
"Niveles de Embalse", one FusionCharts line chart per regulating reservoir - Cachi, Arenal, Pirris and Reventazon -
whose XML holds three daily series for every day of the CURRENT YEAR: "Real" (the observed level, m a.s.l.; the
monthly reports label it "Nivel Real 00:00 horas"), "Programado" (the planned level) and "Programado Anual" (the
annual planning curve). Days after today are 'null'. The panel also has a table with the latest hourly SCADA reading
("Niveles de Embalse <day> - Hora: H:MM"; "Datos tomados del SCADA cada 15 minutos").

Retention: YEAR-TO-DATE ROLLING WINDOW. The chart only shows the current year (subCaption = year, 1 January to 31
December); nothing on the page selects another year, so each year's values disappear when the chart moves to the next
year. On 2026-10-01 the value for that day was already published at 01:00 local time. The Internet Archive holds 33
captures from 2020-09 to 2026-06 (none in 2022); they can be imported with IMPORT_DIR to restore earlier years.

Design:
- Each run fetches the page once. Request-specific parts of the page (time banner, JSF ViewState, chart variable
  names that embed a timestamp) change on every request, so a capture is identified by the hash of the PARSED daily
  chart data and the page is stored only when that hash is new (about once a day, when the day's value appears).
  The hourly table changes every hour; each run appends its reading to raw/hourly_readings.jsonl (one line per
  reading time), so the hourly series does not need a stored page per run.
- CSVs are rebuilt from every stored page on every run: reruns add nothing, and a parser fix can be replayed.
  A day keeps the value of the most recent capture that shows it; a value that later changes is listed in
  costarica_cence_embalses_revisions.csv.
- Observation dates come from the chart's own day labels (dd/mm/yyyy), never from the fetch time.

Licence: the page states "(c) ICE Todos los Derechos Reservados" and gives no reuse permission (reuse_status
`restricted_use`). Collected here by owner decision of 2026-10-01 (same route as CAMMESA); see DATA_LICENCE_NOTICE.md.
Presence in this repository is not permission to republish.

Environment:
  OUTPUT_DIR   output root (default <script_dir>/outputs)
  IMPORT_DIR   optional directory of saved pages named <capture_utc>.html (e.g. Internet Archive timestamps
               20241228075329.html) to add to the archive; the page itself is not fetched when set
  IMPORT_ORIGIN  label recorded for imported pages (default "imported"; "internet_archive" for Wayback copies)

Outputs (under OUTPUT_DIR):
  raw/<YYYY>/<capturedUTC>__<data12>.html.gz      one stored page per distinct data content
  raw/captures.csv                                every stored page: capture time, origin, data hash, chart year
  raw/hourly_readings.jsonl                       the hourly table of every run, one line per reading time
  timeseries/costarica_cence_embalses_daily.csv   days with an observed level: reservoir, observation_date,
                                                  level_real_masl, level_programado_masl, level_programado_anual_masl,
                                                  captured_utc, raw_file
  timeseries/costarica_cence_embalses_planned.csv the planned series for every day the chart shows, including future
                                                  days (column planned_for, so freshness monitors never read a plan
                                                  as an observation date)
  timeseries/costarica_cence_embalses_revisions.csv
  timeseries/costarica_cence_embalses_hourly.csv  reservoir, reading_datetime_local, programado/real at that hour
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import html as html_mod
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

URL = "https://apps.grupoice.com/CenceWeb/CenceBoletinIntraDiario.jsf?init=true"
SOURCE_AGENCY = "ICE - Centro Nacional de Control de Energia (CENCE), DOCSE"
CRT = timezone(timedelta(hours=-6))  # Costa Rica, no daylight saving
TIMEOUT = 90
REQUEST_BACKOFFS = (10, 30, 90)
HEADERS = {"User-Agent": ("Mozilla/5.0 (compatible; global-reservoir-scrapers/1.0; "
                          "+https://github.com/Yuyang16Z/global-reservoir-scrapers)")}

CHART_RE = re.compile(r"graficoEmbalse([A-Za-z]+?)\d+\.setDataXML\(\"(.*?)\"\);", re.S)
CATEGORY_RE = re.compile(r"<category label='([^']*)'")
DATASET_RE = re.compile(r"<dataset seriesName='([^']*)'[^>]*>(.*?)</dataset>", re.S)
SET_RE = re.compile(r"<set value='([^']*)'")
CAPTION_RE = re.compile(r"\bcaption='([^']*)'")
SUBCAPTION_RE = re.compile(r"subCaption='([^']*)'")
TABLE_HEAD_RE = re.compile(r"Niveles de Embalse\s+[^<]*?(\d{1,2})\s+([a-z]{3})\s+(\d{4})\s*-\s*Hora:\s*(\d{1,2}):(\d{2})", re.I)
TABLE_ROW_RE = re.compile(r"<tr data-ri=\"\d+\"[^>]*><td role=\"gridcell\">([^<]+)</td><td role=\"gridcell\">([^<]*)</td>"
                          r"<td role=\"gridcell\">([^<]*)</td></tr>")
MONTHS = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7, "ago": 8, "sep": 9, "set": 9,
          "oct": 10, "nov": 11, "dic": 12}
SERIES = {"Real": "level_real_masl", "Programado": "level_programado_masl", "Programado Anual": "level_programado_anual_masl"}
DAILY_COLUMNS = ["reservoir", "observation_date", "level_real_masl", "level_programado_masl",
                 "level_programado_anual_masl", "captured_utc", "raw_file"]
REVISION_COLUMNS = ["reservoir", "observation_date", "series", "previous_value", "new_value",
                    "previous_captured_utc", "new_captured_utc", "new_raw_file"]
PLANNED_COLUMNS = ["reservoir", "planned_for", "level_programado_masl", "level_programado_anual_masl",
                   "captured_utc", "raw_file"]
HOURLY_COLUMNS = ["reservoir", "reading_datetime_local", "level_programado_masl", "level_real_masl",
                  "captured_utc", "raw_file"]
CAPTURE_COLUMNS = ["raw_file", "captured_utc", "origin", "data_sha256", "chart_year", "first_day", "last_real_day"]


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def number(text: str) -> float | None:
    t = (text or "").strip().replace(",", "")
    if not t or t.lower() in ("null", "n/a", "-"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def parse_page(body: bytes) -> dict:
    """Charts and hourly table of one page: {charts: {reservoir: {caption, year, days: {iso: {series: value}}}},
    table: {reading_local, rows: [...]}}."""
    text = body.decode("utf-8", errors="replace")
    charts = {}
    for name, xml in CHART_RE.findall(text):
        xml = html_mod.unescape(xml)
        caption = (CAPTION_RE.search(xml) or [None, ""])[1]
        year = (SUBCAPTION_RE.search(xml) or [None, ""])[1]
        cats = CATEGORY_RE.findall(xml)
        days: dict[str, dict] = {}
        for series, body_ in DATASET_RE.findall(xml):
            col = SERIES.get(series.strip())
            if not col:
                continue
            for label, raw in zip(cats, SET_RE.findall(body_)):
                try:
                    d = datetime.strptime(label.strip(), "%d/%m/%Y").date().isoformat()
                except ValueError:
                    continue
                v = number(raw)
                if v is not None:
                    days.setdefault(d, {})[col] = v
        reservoir = caption.replace("Embalse", "").strip() or name
        charts[reservoir] = {"chart_id": name, "year": year, "days": days, "n_labels": len(cats)}
    table = {}
    m = TABLE_HEAD_RE.search(text)
    if m:
        day, mon, yr, hh, mm = m.groups()
        if mon.lower() in MONTHS:
            local = datetime(int(yr), MONTHS[mon.lower()], int(day), int(hh), int(mm))
            rows = [{"reservoir": html_mod.unescape(r[0]).strip(), "level_programado_masl": number(r[1]),
                     "level_real_masl": number(r[2])} for r in TABLE_ROW_RE.findall(text)]
            table = {"reading_local": local.isoformat(timespec="minutes"), "rows": rows}
    return {"charts": charts, "table": table}


def data_hash(parsed: dict) -> str:
    """Hash of the daily chart data only (the hourly table changes every hour)."""
    return hashlib.sha256(json.dumps(parsed["charts"], sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def append_hourly(parsed: dict, captured: datetime, origin: str) -> bool:
    table = parsed.get("table") or {}
    if not table.get("rows"):
        return False
    path = RAW_DIR / "hourly_readings.jsonl"
    seen = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                seen.add(json.loads(line)["reading_local"])
            except (ValueError, KeyError):
                continue
    if table["reading_local"] in seen:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"reading_local": table["reading_local"], "rows": table["rows"],
                             "captured_utc": captured.isoformat(timespec="seconds"), "origin": origin},
                            ensure_ascii=False, sort_keys=True) + "\n")
    return True


def fetch() -> tuple[int, bytes, str]:
    last = ""
    for attempt in range(len(REQUEST_BACKOFFS) + 1):
        try:
            r = requests.get(URL, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return 200, r.content, ""
            last = f"HTTP {r.status_code}"
            if r.status_code in (403, 412, 429):
                break  # access control or rate limit: do not hammer
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
        if attempt < len(REQUEST_BACKOFFS):
            time.sleep(REQUEST_BACKOFFS[attempt])
    return 0, b"", last


def read_captures() -> list[dict]:
    path = RAW_DIR / "captures.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def store(body: bytes, captured: datetime, origin: str, captures: list[dict], summary: dict) -> None:
    parsed = parse_page(body)
    if not parsed["charts"]:
        summary["errors"].append(f"{origin} {captured.isoformat()}: no reservoir chart found in the page")
        return
    if append_hourly(parsed, captured, origin):
        summary["new_hourly_readings"] = summary.get("new_hourly_readings", 0) + 1
    h = data_hash(parsed)
    if any(c["data_sha256"] == h for c in captures):
        summary["unchanged"] += 1
        return
    stamp = captured.strftime("%Y%m%dT%H%M%SZ")
    name = f"{captured.year}/{stamp}__{h[:12]}.html.gz"
    (RAW_DIR / name).parent.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / name).write_bytes(gzip.compress(body, mtime=0))
    years = sorted({c["year"] for c in parsed["charts"].values()})
    days = sorted({d for c in parsed["charts"].values() for d in c["days"]})
    real_days = sorted({d for c in parsed["charts"].values() for d, v in c["days"].items() if "level_real_masl" in v})
    captures.append({"raw_file": name, "captured_utc": captured.isoformat(timespec="seconds"), "origin": origin,
                     "data_sha256": h, "chart_year": ";".join(years), "first_day": days[0] if days else "",
                     "last_real_day": real_days[-1] if real_days else ""})
    summary["new_raw_pages"].append(name)


def rebuild(captures: list[dict], summary: dict) -> None:
    daily: dict[tuple[str, str], dict] = {}
    revisions, hourly = [], {}
    hpath = RAW_DIR / "hourly_readings.jsonl"
    if hpath.exists():
        for line in hpath.read_text(encoding="utf-8").splitlines():
            try:
                t = json.loads(line)
            except ValueError:
                continue
            for r in t.get("rows", []):
                hourly[(r["reservoir"], t["reading_local"])] = {
                    "reservoir": r["reservoir"], "reading_datetime_local": t["reading_local"],
                    "level_programado_masl": r["level_programado_masl"], "level_real_masl": r["level_real_masl"],
                    "captured_utc": t["captured_utc"], "raw_file": "hourly_readings.jsonl"}
    for cap in sorted(captures, key=lambda c: (c["captured_utc"], c["raw_file"])):
        parsed = parse_page(gzip.decompress((RAW_DIR / cap["raw_file"]).read_bytes()))
        for res, chart in parsed["charts"].items():
            for d, vals in chart["days"].items():
                key = (res, d)
                old = daily.get(key)
                if old:
                    for col, v in vals.items():
                        if old.get(col) is not None and old[col] != v:
                            revisions.append({"reservoir": res, "observation_date": d, "series": col,
                                              "previous_value": old[col], "new_value": v,
                                              "previous_captured_utc": old["captured_utc"],
                                              "new_captured_utc": cap["captured_utc"], "new_raw_file": cap["raw_file"]})
                row = dict(old or {"reservoir": res, "observation_date": d})
                row.update(vals)
                row.update(captured_utc=cap["captured_utc"], raw_file=cap["raw_file"])
                daily[key] = row
    rows = sorted(daily.values(), key=lambda r: (r["reservoir"], r["observation_date"]))
    observed = [r for r in rows if r.get("level_real_masl") is not None]
    planned = [{"reservoir": r["reservoir"], "planned_for": r["observation_date"],
                "level_programado_masl": r.get("level_programado_masl"),
                "level_programado_anual_masl": r.get("level_programado_anual_masl"),
                "captured_utc": r["captured_utc"], "raw_file": r["raw_file"]}
               for r in rows if r.get("level_programado_masl") is not None or r.get("level_programado_anual_masl") is not None]
    write_csv(TS_DIR / "costarica_cence_embalses_daily.csv", DAILY_COLUMNS, observed)
    write_csv(TS_DIR / "costarica_cence_embalses_planned.csv", PLANNED_COLUMNS, planned)
    write_csv(TS_DIR / "costarica_cence_embalses_revisions.csv", REVISION_COLUMNS, revisions)
    write_csv(TS_DIR / "costarica_cence_embalses_hourly.csv", HOURLY_COLUMNS,
              sorted(hourly.values(), key=lambda r: (r["reservoir"], r["reading_datetime_local"])))
    write_csv(RAW_DIR / "captures.csv", CAPTURE_COLUMNS, sorted(captures, key=lambda c: c["captured_utc"]))
    real = [r for r in rows if r.get("level_real_masl") is not None]
    by_res = {}
    for r in real:
        by_res.setdefault(r["reservoir"], []).append(r["observation_date"])
    summary.update(stored_pages=len(captures), daily_rows=len(observed), planned_rows=len(planned),
                   real_level_rows=len(real), revisions=len(revisions),
                   reservoirs={k: {"days": len(v), "first": min(v), "last": max(v)} for k, v in sorted(by_res.items())},
                   latest_observation_date=max((r["observation_date"] for r in real), default=None))


def main() -> int:
    started = utc_now()
    stamp = started.strftime("%Y-%m-%dT%H%M%SZ")
    summary = {"source": "costarica/cence_embalses", "agency": SOURCE_AGENCY, "url": URL,
               "started_utc": started.isoformat(timespec="seconds"), "http_status": None, "unchanged": 0,
               "new_raw_pages": [], "errors": []}
    captures = read_captures()
    import_dir = os.environ.get("IMPORT_DIR", "").strip()
    if import_dir:
        for p in sorted(Path(import_dir).glob("*.html")):
            try:
                captured = datetime.strptime(p.stem[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            except ValueError:
                summary["errors"].append(f"{p.name}: file name is not a capture time (YYYYMMDDhhmmss)")
                continue
            store(p.read_bytes(), captured, os.environ.get("IMPORT_ORIGIN", "").strip() or "imported", captures, summary)
        summary["http_status"] = "import"
    else:
        log(f"CENCE: fetching {URL}")
        status, body, err = fetch()
        summary["http_status"] = status or err
        if status == 200:
            store(body, utc_now(), "live", captures, summary)
        else:
            summary["errors"].append(f"fetch failed: {err}")
    if captures:
        rebuild(captures, summary)
    summary["finished_utc"] = utc_now().isoformat(timespec="seconds")
    ok_fetch = summary["http_status"] in (200, "import")
    summary["status"] = "failed" if not ok_fetch or not captures else ("partial" if summary["errors"] else "ok")
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1) + "\n",
                                                       encoding="utf-8")
    log(json.dumps(summary, ensure_ascii=False))
    if summary["status"] == "failed":
        log("ERROR: the page could not be retrieved or parsed in this run.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

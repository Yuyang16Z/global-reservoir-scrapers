"""İZSU (İzmir, Turkey) - daily water status of İzmir's drinking-water dams, from the city's open-data API.

Source (dataset "Barajların Doluluk Oranları" of İzmir Büyükşehir Belediyesi's open-data portal,
https://acikveri.bizizmir.com/dataset/barajlarin-doluluk-oranlari):
  https://acikveriwapi.izmir.bel.tr/AcikVeriIzsuApi/DamWaterStatus                   the latest day
  https://acikveriwapi.izmir.bel.tr/AcikVeriIzsuApi/DamWaterStatus?year=YYYY         every day of a year
  https://acikveriwapi.izmir.bel.tr/AcikVeriIzsuApi/DamWaterStatus?date=YYYY-MM-DD   one day

İZSU (İzmir Su ve Kanalizasyon İdaresi) is the water and sewerage administration of İzmir. An answer is
{"data": [...], "statusCode": 200, "ok": true, "errorMessage": null}; each record has `date`, `waterStatus`,
`usableWaterStatus`, `occupancyRate`, `waterLevel` and `damWellFacility` {`id`, `name`}. The dataset describes them as
the dams' daily water volume, fill ratio and water elevation ("günlük su hacmi, doluluk oranı ve su kotu"); the answer
itself states no units. The first read, on 2026-11-27, found 36,549 records of eight facilities from 2009-03-12 to
2026-11-27 (among them Güzelhisar Barajı and Karaçam Göleti); the latest day listed five dams (Tahtalı, Balçova,
Ürkmez, Gördes, Alaçatı Kutlu Aktaş).

Retention: PERMANENT ARCHIVE, as far as observed. On 2026-11-27 every year from 2009 to 2026 was served on request
and 2006, 2007 and 2008 came back empty; the API accepts years from 1900 to the current year plus one and answers
HTTP 400 outside that. A missed run therefore loses nothing. This copy exists because the portal's licence page says
"Data providers do not guarantee the continuity of data", and because the API this one replaced
(openapi.izmir.bel.tr/api/izsu/barajdurum) answers HTTP 500 since November 2026.

Design:
- Requests are made one at a time, PAUSE_SECONDS apart: sixteen concurrent requests on 2026-11-27 got HTTP 502
  ("Service is not reachable") for two of them.
- Every run asks for the current year (and, in January, the previous one) and compares each date with the archive.
  A date that is new or whose values differ is then asked for on its own (`?date=`) and that answer is stored as
  served. More than MAX_DATE_REQUESTS such dates (the first run, or the first run after a long gap) store the year
  answer instead. The two kinds of answer held identical records for the three dates compared on 2026-11-27
  (2026-01-01, 2026-06-15, 2026-11-27); a date where they differ is reported as `endpoint_differences`.
- Every earlier year is asked for once and its answer stored, empty or not, walking back from the current year until
  EMPTY_YEARS_STOP years in a row are empty (21 requests in 2 min 15 s on 2026-11-27). A year whose request fails
  is asked for again by the next run, so the history completes itself. REREAD_ALL_YEARS=1 asks for the stored years again and stores an answer that differs.
- A stored answer is never overwritten. Tables are rebuilt from the stored answers on every run, so reruns are
  idempotent and a parser fix can be replayed over the whole archive. The values of a date that change are kept as a
  new version; the revisions table lists every version of such a date. A record's own `id` is not part of that
  comparison.
- Numbers are kept as the text the API sent (135338000.00 stays 135338000.00). Nothing is converted, checked against
  other fields or blanked here; that happens in the project's delivery layer.
- A date the API no longer lists is reported (`dates_no_longer_served`) and kept.
- HTTP 403/412/429 stops the run (the project does not work around blocks).

Licence: CC BY 4.0. The portal's licence page (https://acikveri.bizizmir.com/en/license, checked 2026-11-27) reads
"All materials under this site are licensed by Izmir Metropolitan Municipality under CC BY 4.0 international license,
unless otherwise stated", and the dataset's own licence field names that page (reuse_status `open_attribution`).
Attribution: "Source: İzmir Büyükşehir Belediyesi Açık Veri Portalı / İZSU, CC BY 4.0."

Environment:
  OUTPUT_DIR          output root (default <script_dir>/outputs)
  PAUSE_SECONDS       pause between requests (default 3)
  MAX_DATE_REQUESTS   most single-date requests in a run before the year answer is stored instead (default 40)
  REREAD_ALL_YEARS    "1" asks for every stored year again

Outputs (under OUTPUT_DIR):
  raw/years/<YYYY>_<capturedUTC>__<body12>.json.gz   a year answer as served
  raw/<YYYY>/<YYYYMMDD>_<capturedUTC>__<data12>.json  a single-date answer as served
  raw/static/<name>_<capturedUTC>__<body12>.csv       the dataset's CSV of capacities and coordinates
  raw/captures.csv                                    every stored answer: capture time, hash, dates, records
  timeseries/turkey_izsu_<YYYY>.csv                   latest version of each date of that year
  timeseries/turkey_izsu_revisions.csv                every version of the dates that have more than one
  metadata/turkey_izsu_reservoirs.csv                 one row per facility
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
META_DIR = OUTPUT_DIR / "metadata"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

ENDPOINT = "https://acikveriwapi.izmir.bel.tr/AcikVeriIzsuApi/DamWaterStatus"
STATIC_CSV_URL = "https://openfiles.izmir.bel.tr/309436/docs/izsu-baraj-doluluk-oranlari.csv"
STATIC_NAME = "izsu-baraj-doluluk-oranlari"
SOURCE_AGENCY = "İZSU (İzmir Su ve Kanalizasyon İdaresi Genel Müdürlüğü), İzmir, Turkey"
SOURCE_TZ = ZoneInfo("Europe/Istanbul")
FIRST_YEAR = 1900                     # the API accepts 1900 .. current year + 1
EMPTY_YEARS_STOP = 3
TIMEOUT = 90
REQUEST_BACKOFFS = (5, 20, 60)
PAUSE_SECONDS = float(os.environ.get("PAUSE_SECONDS") or 3)
MAX_DATE_REQUESTS = int(os.environ.get("MAX_DATE_REQUESTS") or 40)
REREAD_ALL_YEARS = os.environ.get("REREAD_ALL_YEARS", "").strip() == "1"
BLOCKED = (403, 412, 429)
HEADERS = {"User-Agent": ("Mozilla/5.0 (compatible; global-reservoir-scrapers/1.0; "
                          "+https://github.com/Yuyang16Z/global-reservoir-scrapers)"),
           "Accept": "application/json"}

FIELDS = ["waterStatus", "usableWaterStatus", "occupancyRate", "waterLevel"]
COLUMNS = ["reservoir_id", "reservoir_name", "date", *FIELDS, "unparsed_fields", "version", "n_versions",
           "raw_file"]
CAPTURE_COLUMNS = ["raw_file", "kind", "scope", "captured_utc", "body_sha256", "bytes", "records", "dates",
                   "first_date", "last_date", "problem"]
STATIC_FIELDS = ["BARAJ_KUYU_ID", "MAKSIMUM_SU_KAPASITESI", "MINIMUM_SU_KAPASITESI", "KULLANILABILIR_GOL_SU_HACMI",
                 "MAKSIMUM_SU_YUKSEKLIGI", "MINIMUM_SU_YUKSEKLIGI"]
META_COLUMNS = ["reservoir_id", "reservoir_name", "reservoir_name_en", "country", "admin_unit", "river", "basin",
                "lat", "lon", "source_agency", "source_url", "data_type", "last_updated", "first_date",
                *STATIC_FIELDS, "static_file"]
TS_NAME = "turkey_izsu"

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YEAR_FILE = re.compile(r"^(\d{4})_(\d{8}T\d{6}Z)__([0-9a-f]{12})\.json\.gz$")
DATE_FILE = re.compile(r"^(\d{8})_(\d{8}T\d{6}Z)__([0-9a-f]{12})\.json$")
STATIC_FILE = re.compile(r"^" + re.escape(STATIC_NAME) + r"_(\d{8}T\d{6}Z)__([0-9a-f]{12})\.csv$")


def log(msg: str) -> None:
    print(msg, flush=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_now() -> str:
    return now_utc().isoformat(timespec="seconds")


def stamp_now() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def stamp_iso(stamp: str) -> str:
    return datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat(timespec="seconds")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch(url: str, backoffs: tuple = REQUEST_BACKOFFS) -> tuple[int, bytes, str]:
    """GET with bounded retries on network errors and server errors; any other answer is returned at once."""
    last = ""
    for attempt in range(len(backoffs) + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return 200, r.content, ""
            last = f"HTTP {r.status_code}"
            if r.status_code < 500:
                detail = r.content[:160].decode("utf-8", "replace").strip()
                return r.status_code, b"", f"{last} {detail}".strip()
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
        if attempt < len(backoffs):
            time.sleep(backoffs[attempt])
    return 0, b"", last


def read_answer(body: bytes) -> tuple[dict[str, list[dict]] | None, str]:
    """(records per date, "") for a usable answer, else (None, what is wrong with it).

    Numbers stay the text the API sent. Records without a YYYY-MM-DD date are returned under "".
    """
    try:
        doc = json.loads(body.decode("utf-8-sig"), parse_float=str, parse_int=str)
    except (ValueError, UnicodeDecodeError):
        return None, "answer is not JSON"
    if not isinstance(doc, dict):
        return None, "answer is not a JSON object"
    if doc.get("ok") is False or str(doc.get("statusCode", "200")) != "200":
        return None, f"API answered statusCode {doc.get('statusCode')}: {doc.get('errorMessage')}"
    data = doc.get("data")
    if not isinstance(data, list):
        return None, "answer has no data list"
    days: dict[str, list[dict]] = {}
    for rec in data:
        if not isinstance(rec, dict):
            rec = {"value": rec}
        day = str(rec.get("date") or "")[:10]
        days.setdefault(day if DAY_RE.match(day) else "", []).append(rec)
    return days, ""


def data_hash(records: list[dict]) -> str:
    """sha256 of one date's records, whatever their order; a record's own `id` is left out."""
    parts = sorted(json.dumps({k: v for k, v in rec.items() if k != "id"}, sort_keys=True, ensure_ascii=False,
                              separators=(",", ":")) for rec in records)
    return sha256("\n".join(parts).encode("utf-8"))


def containers() -> list[dict]:
    """Every stored answer, oldest capture first."""
    out = []
    for p in sorted(RAW_DIR.rglob("*.json*")) if RAW_DIR.is_dir() else []:
        m = YEAR_FILE.match(p.name) or DATE_FILE.match(p.name)
        if not m:
            continue
        kind = "year" if p.name.endswith(".gz") else "date"
        try:
            body = gzip.decompress(p.read_bytes()) if kind == "year" else p.read_bytes()
        except (OSError, EOFError):
            body = b""
        days, problem = read_answer(body)
        out.append({"raw_file": p.relative_to(RAW_DIR).as_posix(), "kind": kind, "scope": m.group(1),
                    "captured": stamp_iso(m.group(2)), "body_sha256": sha256(body), "bytes": len(body),
                    "days": days, "problem": problem})
    out.sort(key=lambda c: (c["captured"], c["raw_file"]))
    return out


def versions_by_date(stored: list[dict]) -> dict[str, list[dict]]:
    """date -> its versions in capture order; a version starts where the date's values differ from the one before."""
    out: dict[str, list[dict]] = {}
    for c in stored:
        for day, records in (c["days"] or {}).items():
            if not day:
                continue
            h = data_hash(records)
            versions = out.setdefault(day, [])
            if not versions or versions[-1]["hash"] != h:
                versions.append({"hash": h, "records": records, "raw_file": c["raw_file"],
                                 "captured": c["captured"]})
    return out


def source_today() -> date:
    return now_utc().astimezone(SOURCE_TZ).date()


def capture(summary: dict) -> None:
    """Read the current year, complete the earlier years, and store what the archive does not hold yet."""
    stored = containers()
    latest = {day: versions[-1]["hash"] for day, versions in versions_by_date(stored).items()}
    covered: dict[int, bool] = {}              # year -> its latest stored year answer has records
    for c in stored:
        if c["kind"] == "year" and c["days"] is not None:
            covered[int(c["scope"])] = any(c["days"])
    today = source_today()
    window = {today.year} | ({today.year - 1} if today.month == 1 else set())
    empty_streak, asked = 0, False

    for year in range(today.year, FIRST_YEAR - 1, -1):
        in_window = year in window
        if not in_window and year in covered and not REREAD_ALL_YEARS:
            empty_streak = 0 if covered[year] else empty_streak + 1
            if empty_streak >= EMPTY_YEARS_STOP:
                break
            continue
        if asked:
            time.sleep(PAUSE_SECONDS)
        asked = True
        status, body, err = fetch(f"{ENDPOINT}?year={year}")
        entry: dict = {"year": year, "http_status": status, "bytes": len(body)}
        summary["year_requests"].append(entry)
        days, problem = read_answer(body) if status == 200 else (None, err)
        if days is None:
            entry["result"] = problem
            log(f"year {year}: {problem}")
            summary["errors"].append(f"year {year}: {problem}")
            summary["blocked"] = summary["blocked"] or status in BLOCKED
            break                              # a year that fails is asked for again by the next run
        summary["years_answered"] += 1
        dated = {day: records for day, records in days.items() if day}
        summary["undated_records"] += len(days.get("", []))
        entry.update(records=sum(len(r) for r in days.values()), dates=len(dated),
                     first_date=min(dated) if dated else None, last_date=max(dated) if dated else None)
        summary["date_mismatches"] += [f"year {year} answer lists {day}"
                                       for day in sorted(dated) if not day.startswith(f"{year}-")]
        summary["dates_no_longer_served"] += sorted(day for day in latest
                                                    if day.startswith(f"{year}-") and day not in dated)
        hashes = {day: data_hash(records) for day, records in dated.items()}
        changed = sorted(day for day, h in hashes.items() if latest.get(day) != h)
        entry["changed_dates"] = len(changed)
        empty_streak = 0 if dated else empty_streak + 1

        if (not in_window and year not in covered) or (changed and (not in_window
                                                                    or len(changed) > MAX_DATE_REQUESTS)):
            name = f"{year}_{stamp_now()}__{sha256(body)[:12]}.json.gz"
            path = RAW_DIR / "years" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(gzip.compress(body, mtime=0))
            latest.update(hashes)
            covered[year] = bool(dated)
            entry["result"] = f"years/{name}"
            summary["new_raw_files"].append(f"years/{name}")
        elif changed:
            entry["result"] = "asked for the changed dates"
            if not capture_dates(summary, changed, hashes, latest):
                break
        else:
            entry["result"] = "unchanged"
        log(f"year {year}: {entry['records']} records, {entry['dates']} dates "
            f"({entry['first_date']} to {entry['last_date']}), {len(changed)} new or changed -> {entry['result']}")
        if empty_streak >= EMPTY_YEARS_STOP:
            break


def capture_dates(summary: dict, days: list[str], year_hashes: dict[str, str], latest: dict[str, str]) -> bool:
    """Ask for each date on its own and store the answers that are new. False when the run should stop."""
    for day in days:
        time.sleep(PAUSE_SECONDS)
        status, body, err = fetch(f"{ENDPOINT}?date={day}")
        entry: dict = {"date": day, "http_status": status, "bytes": len(body)}
        summary["date_requests"].append(entry)
        answer, problem = read_answer(body) if status == 200 else (None, err)
        if answer is None:
            entry["result"] = problem
            summary["errors"].append(f"{day}: {problem}")
            summary["blocked"] = summary["blocked"] or status in BLOCKED
            return False                       # the next run finds the same dates changed and asks again
        records = answer.get(day)
        others = sorted(k for k in answer if k != day)
        if others:
            summary["date_mismatches"].append(f"date {day} answer also lists {', '.join(others[:5]) or 'undated'}")
        if not records:
            entry["result"] = "no record for that date"
            summary["errors"].append(f"{day}: the date query returned no record for that date")
            continue
        h = data_hash(records)
        if h != year_hashes.get(day):
            summary["endpoint_differences"].append(day)
        if latest.get(day) == h:
            entry["result"] = "unchanged"
            continue
        name = f"{day.replace('-', '')}_{stamp_now()}__{h[:12]}.json"
        path = RAW_DIR / day[:4] / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        latest[day] = h
        entry["result"] = f"{day[:4]}/{name}"
        summary["new_raw_files"].append(f"{day[:4]}/{name}")
    return True


def capture_static(summary: dict) -> None:
    """Store the dataset's CSV of capacities and coordinates once per distinct content. Optional."""
    status, body, err = fetch(STATIC_CSV_URL, backoffs=())
    if status != 200 or b"BARAJ_KUYU_ADI" not in body[:400]:
        summary["static_csv"] = f"not usable: {err or 'unexpected content'}"
        return
    digest = sha256(body)[:12]
    static_dir = RAW_DIR / "static"
    if any(p.name.endswith(f"__{digest}.csv") for p in static_dir.glob("*.csv")):
        summary["static_csv"] = "unchanged"
        return
    name = f"{STATIC_NAME}_{stamp_now()}__{digest}.csv"
    static_dir.mkdir(parents=True, exist_ok=True)
    (static_dir / name).write_bytes(body)
    summary["static_csv"] = f"static/{name}"
    summary["new_raw_files"].append(f"static/{name}")


def text(value) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)


def record_row(day: str, rec: dict) -> dict:
    facility = rec.get("damWellFacility") if isinstance(rec.get("damWellFacility"), dict) else {}
    extra = {k: v for k, v in rec.items() if k not in ("id", "date", "damWellFacility", *FIELDS)}
    facility_extra = {k: v for k, v in facility.items() if k not in ("id", "name")}
    if facility_extra or not facility and "damWellFacility" in rec:
        extra["damWellFacility"] = facility_extra or rec["damWellFacility"]
    return {"reservoir_id": text(facility.get("id")), "reservoir_name": text(facility.get("name")), "date": day,
            **{f: text(rec.get(f)) for f in FIELDS},
            "unparsed_fields": json.dumps(extra, ensure_ascii=False, sort_keys=True) if extra else ""}


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def static_attributes() -> tuple[dict[str, dict], str, str]:
    """(facility name -> row of the newest stored static CSV, its file, its capture time)."""
    files = sorted((STATIC_FILE.match(p.name).group(1), p) for p in (RAW_DIR / "static").glob("*.csv")
                   if STATIC_FILE.match(p.name)) if (RAW_DIR / "static").is_dir() else []
    if not files:
        return {}, "", ""
    stamp, path = files[-1]
    rows = csv.DictReader(path.read_text(encoding="utf-8-sig").splitlines(), delimiter=";")
    by_name = {(r.get("BARAJ_KUYU_ADI") or "").strip().casefold(): r for r in rows}
    return by_name, f"static/{path.name}", stamp_iso(stamp)


def rebuild(summary: dict) -> None:
    stored = containers()
    versions = versions_by_date(stored)
    by_year: dict[str, list[dict]] = {}
    revisions, after_capture = [], []
    facilities: dict[str, dict] = {}           # id -> name on its latest date, first date
    for day in sorted(versions):
        kept = []
        for v in versions[day]:
            captured_on = datetime.fromisoformat(v["captured"]).astimezone(SOURCE_TZ).date()
            if date.fromisoformat(day) > captured_on + timedelta(days=1):
                after_capture.append(f"{day} in {v['raw_file']}")
            else:
                kept.append(v)
        for i, v in enumerate(kept, start=1):
            rows = sorted((record_row(day, rec) for rec in v["records"]),
                          key=lambda r: (r["reservoir_name"], r["reservoir_id"]))
            for r in rows:
                r.update(version=i, n_versions=len(kept), raw_file=v["raw_file"])
            if len(kept) > 1:
                revisions += rows
            if i == len(kept):
                by_year.setdefault(day[:4], []).extend(rows)
                for r in rows:
                    if r["reservoir_id"]:
                        f = facilities.setdefault(r["reservoir_id"], {"first_date": day})
                        f["name"] = r["reservoir_name"]

    for year, rows in by_year.items():
        write_csv(TS_DIR / f"{TS_NAME}_{year}.csv", COLUMNS, rows)
    write_csv(TS_DIR / f"{TS_NAME}_revisions.csv", COLUMNS, revisions)

    captures = []
    for c in stored:
        dated = sorted(day for day in (c["days"] or {}) if day)
        captures.append({"raw_file": c["raw_file"], "kind": c["kind"], "scope": c["scope"],
                         "captured_utc": c["captured"], "body_sha256": c["body_sha256"], "bytes": c["bytes"],
                         "records": sum(len(r) for r in (c["days"] or {}).values()), "dates": len(dated),
                         "first_date": dated[0] if dated else "", "last_date": dated[-1] if dated else "",
                         "problem": c["problem"]})
    if captures:
        write_csv(RAW_DIR / "captures.csv", CAPTURE_COLUMNS, captures)

    static, static_file, static_time = static_attributes()
    meta = []
    for fid, f in sorted(facilities.items(), key=lambda kv: (kv[1]["name"], kv[0])):
        s = static.get(f["name"].strip().casefold(), {})
        meta.append({"reservoir_id": fid, "reservoir_name": f["name"], "reservoir_name_en": "", "country": "Turkey",
                     "admin_unit": "İzmir", "river": "", "basin": "", "lat": s.get("ENLEM", ""),
                     "lon": s.get("BOYLAM", ""), "source_agency": "İZSU", "source_url": ENDPOINT,
                     "data_type": "in_situ", "last_updated": static_time if s else "",
                     "first_date": f["first_date"], **{k: s.get(k, "") for k in STATIC_FIELDS},
                     "static_file": static_file if s else ""})
    if meta:
        write_csv(META_DIR / f"{TS_NAME}_reservoirs.csv", META_COLUMNS, meta)

    days = sorted(day for rows in by_year.values() for day in {r["date"] for r in rows})
    summary.update(
        stored_answers=len(stored), unreadable_answers=[c["raw_file"] for c in stored if c["days"] is None],
        record_dates=len(days), earliest_record_date=days[0] if days else None,
        latest_record_date=days[-1] if days else None, rows=sum(len(rows) for rows in by_year.values()),
        facilities=len(facilities), dates_with_revisions=sorted(d for d, v in versions.items() if len(v) > 1),
        dates_after_capture=after_capture,
        years={year: {"dates": len({r["date"] for r in rows}), "rows": len(rows)}
               for year, rows in sorted(by_year.items())})


def main() -> int:
    stamp = now_utc().strftime("%Y-%m-%dT%H%M%SZ")
    summary: dict = {"source": "turkey/izsu", "agency": SOURCE_AGENCY, "started_utc": utc_now(),
                     "endpoint": ENDPOINT, "reread_all_years": REREAD_ALL_YEARS, "years_answered": 0,
                     "year_requests": [], "date_requests": [], "new_raw_files": [], "undated_records": 0,
                     "date_mismatches": [], "dates_no_longer_served": [], "endpoint_differences": [],
                     "errors": [], "blocked": False}
    log("İZSU dam water status: reading the current year and completing the earlier ones")
    capture(summary)
    if summary["years_answered"] and not summary["blocked"]:
        capture_static(summary)
    rebuild(summary)
    summary["finished_utc"] = utc_now()
    problems = ("errors", "date_mismatches", "dates_no_longer_served", "endpoint_differences",
                "unreadable_answers", "dates_after_capture")
    if summary["years_answered"] == 0 or summary["blocked"]:
        summary["status"] = "failed"
    elif any(summary[k] for k in problems) or summary["undated_records"]:
        summary["status"] = "partial"
    else:
        summary["status"] = "ok"
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    brief = {k: v for k, v in summary.items() if k not in ("year_requests", "date_requests", "years")}
    for key in ("dates_no_longer_served", "dates_with_revisions", "dates_after_capture", "new_raw_files"):
        if len(brief.get(key) or []) > 12:
            brief[key] = [*brief[key][:12], f"... {len(summary[key]) - 12} more"]
    log(json.dumps(brief, ensure_ascii=False))
    if summary["status"] == "failed":
        log("ERROR: no year could be read in this run (API down, URL changed or blocked).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

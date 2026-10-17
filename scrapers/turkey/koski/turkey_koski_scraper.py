"""KOSKİ (Konya, Turkey) - dated readings of four water-supply dams on the "Baraj Değerleri" pages.

Source (one page per dam):
  https://www.koski.gov.tr/koski/altinapa-baraj-degerleri
  https://www.koski.gov.tr/koski/bagbasi-baraj-degerleri
  https://www.koski.gov.tr/koski/bozkir-baraj-degerleri
  https://www.koski.gov.tr/koski/afsar-baraj-degerleri

KOSKİ (Konya Su ve Kanalizasyon İdaresi, the water and sewerage administration of Konya) shows, per dam, one static
HTML table (id "databaraj") of dated readings: 'Tarih' (date, DD/MM/YYYY), 'Yükseklik' (height: a gauge reading in cm
at Altınapa, the level in whole metres at the other dams), 'Kot' (water-surface elevation, m), 'Hacim' (volume, m3)
and 'Doluluk Oranı' (fill ratio, %). Internet Archive captures from 2020 print 'Gelen CM' and 'Gelen LT' (incoming)
where today's pages print 'Doluluk Oranı', so cells are read by their header names, never by position.

Retention: ROLLING WINDOW of six years. On the captures of 2026-09-08, 2026-09-29 and 2026-10-14 the Altınapa and
Bağbaşı tables started six years before the capture day in Istanbul time (2020-09-08, 2020-09-30, 2020-10-14), so an
early date leaves the page every day. Bozkır and Afşar start at 2022-01-01, inside that window so far. No published
value changed between those captures. Nothing on the site serves the dates that have left the pages.

Design:
- Each run fetches the four pages, PAUSE_SECONDS apart. A page is stored only when its table differs from every stored
  page of that dam (hash of the table cells, not of the HTML), so a rerun stores nothing.
- A page whose readings table cannot be read is still stored, under raw/unparsed/ (once per distinct content), and the
  run ends as "partial", so a layout change loses no reading and is visible in the run log.
- The CSV is rebuilt from every stored page on every run, keyed on (dam, measurement_date) where measurement_date is
  the table's own 'Tarih', never the fetch time; a later capture wins and a value that changes is listed in the
  revisions CSV. A column the later page does not print keeps the earlier page's value (pages before 2026-03 print
  'Gelen CM'/'Gelen LT', later ones 'Doluluk Oranı'), so raw_file names the latest page of the date only. A row dated
  more than a day after its capture is reported and not kept.
- Cell text is kept exactly as published. Nothing is converted, checked against other columns or blanked here; that
  happens in the project's delivery layer.
- HTTP 403/412/429 stops the run (the project does not work around blocks).

Licence: KOSKİ publishes no reuse or redistribution terms; the site footer reads "© 2026 | www.koski.gov.tr Tüm Hakları
Saklıdır" (All Rights Reserved) (reuse_status `undeclared_review`, checked 2026-09-08). Collected here by owner
decision; see DATA_LICENCE_NOTICE.md. Presence in this repository is not permission to republish.

Environment:
  OUTPUT_DIR     output root (default <script_dir>/outputs)
  PAUSE_SECONDS  pause between the four page requests (default 3)
  IMPORT_DIR     optional directory of saved pages named <slug>_<YYYYmmddTHHMMSS>.html or <slug>_<YYYYmmddHHMMSS>.html
                 (capture time, UTC; e.g. altinapa_20260908T070343.html or an Internet Archive timestamp), added to
                 the archive instead of fetching
  IMPORT_ORIGIN  label recorded for imported pages (default "imported"; e.g. "local_capture", "internet_archive")

Outputs (under OUTPUT_DIR):
  raw/<YYYY>/<slug>_<capturedUTC>__<data12>.html.gz   one stored page per dam and distinct table
  raw/unparsed/<slug>_<capturedUTC>__<sha12>.html.gz  pages without a readable table (once per distinct content)
  raw/captures.csv                                    every stored page: dam, capture time, origin, data hash,
                                                      columns, rows, first and last date
  timeseries/turkey_koski.csv                         dam, measurement_date, one column per page column (verbatim
                                                      text), captured_utc, raw_file
  timeseries/turkey_koski_revisions.csv               values that changed between captures
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import html
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

URL_TMPL = "https://www.koski.gov.tr/koski/{slug}-baraj-degerleri"
DAMS = {"altinapa": "Altınapa Barajı", "bagbasi": "Bağbaşı Barajı",
        "bozkir": "Bozkır Barajı", "afsar": "Afşar Barajı"}
SOURCE_AGENCY = "KOSKİ (Konya Su ve Kanalizasyon İdaresi Genel Müdürlüğü), Konya, Turkey"
TIMEOUT = 60
REQUEST_BACKOFFS = (10, 30, 90)
PAUSE_SECONDS = float(os.environ.get("PAUSE_SECONDS") or 3)
BLOCKED = (403, 412, 429)
HEADERS = {"User-Agent": ("Mozilla/5.0 (compatible; global-reservoir-scrapers/1.0; "
                          "+https://github.com/Yuyang16Z/global-reservoir-scrapers)"),
           "Accept-Language": "tr,en;q=0.8"}

# CSV column for each page column; any other header gets a transliterated name.
KNOWN_COLUMNS = {"tarih": "measurement_date", "yükseklik": "yukseklik", "kot": "kot", "hacim": "hacim",
                 "doluluk oranı": "doluluk_orani", "gelen cm": "gelen_cm", "gelen lt": "gelen_lt"}
VALUE_COLUMNS = ["yukseklik", "kot", "hacim", "doluluk_orani"]
TURKISH = str.maketrans({"ı": "i", "İ": "i", "ş": "s", "Ş": "s", "ğ": "g", "Ğ": "g", "ü": "u", "Ü": "u",
                         "ö": "o", "Ö": "o", "ç": "c", "Ç": "c"})
CAPTURE_COLUMNS = ["dam", "raw_file", "captured_utc", "origin", "data_sha256", "columns", "rows",
                   "first_date", "last_date"]
REVISION_COLUMNS = ["dam", "measurement_date", "field", "previous_value", "new_value",
                    "previous_captured_utc", "new_captured_utc", "new_raw_file"]
TS_NAME = "turkey_koski"


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _text(cell: str) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", "", cell)).replace("\xa0", " ").split())


def column_name(header: str) -> str:
    key = header.casefold()
    if key in KNOWN_COLUMNS:
        return KNOWN_COLUMNS[key]
    return re.sub(r"[^a-z0-9]+", "_", header.translate(TURKISH).lower()).strip("_") or "column"


def parse_page(body: bytes) -> dict:
    """{columns: [csv names], rows: [{measurement_date, <column>: text}], problems: [...]} of the readings table."""
    page = body.decode("utf-8", errors="replace")
    m = re.search(r"<table[^>]*\bid=[\"']databaraj[\"'][^>]*>(.*?)</table>", page, flags=re.S | re.I)
    if not m:
        return {"columns": [], "rows": [], "problems": ["no table with id 'databaraj'"]}
    block, headers = m.group(1), []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", block, flags=re.S | re.I):
        headers = [_text(c) for c in re.findall(r"<th[^>]*>(.*?)</th>", tr, flags=re.S | re.I)]
        if headers:
            break
    columns = [column_name(h) for h in headers]
    if "measurement_date" not in columns or len(set(columns)) != len(columns):
        return {"columns": columns, "rows": [], "problems": [f"unexpected table header {headers}"]}
    rows, problems = [], []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", block, flags=re.S | re.I):
        cells = [_text(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S | re.I)]
        if not cells:
            continue
        if len(cells) != len(columns):
            problems.append(f"row with {len(cells)} cells under {len(columns)} columns: {cells[:3]}")
            continue
        row = dict(zip(columns, cells))
        try:
            row["measurement_date"] = datetime.strptime(row["measurement_date"], "%d/%m/%Y").date().isoformat()
        except ValueError:
            problems.append(f"unreadable date {row['measurement_date']!r}")
            continue
        rows.append(row)
    return {"columns": columns, "rows": rows, "problems": problems}


def data_hash(parsed: dict) -> str:
    rows = sorted(parsed["rows"], key=lambda r: r["measurement_date"])
    return hashlib.sha256(json.dumps({"columns": parsed["columns"], "rows": rows}, sort_keys=True,
                                     ensure_ascii=False).encode("utf-8")).hexdigest()


def fetch(url: str) -> tuple[int, bytes, str]:
    """GET with bounded retries on network and server errors; a block is returned at once."""
    last = ""
    for attempt in range(len(REQUEST_BACKOFFS) + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return 200, r.content, ""
            last = f"HTTP {r.status_code}"
            if r.status_code in BLOCKED:
                return r.status_code, b"", last  # access control or rate limit: do not hammer
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


def store(slug: str, body: bytes, captured: datetime, origin: str, captures: list[dict], summary: dict) -> None:
    parsed = parse_page(body)
    stamp = captured.strftime("%Y%m%dT%H%M%SZ")
    for p in parsed["problems"][:20]:
        summary["errors"].append(f"{slug} {origin} {stamp}: {p}")
    late = (captured.date() + timedelta(days=1)).isoformat()
    future = [r for r in parsed["rows"] if r["measurement_date"] > late]
    for r in future:
        summary["errors"].append(f"{slug} {origin} {stamp}: row dated {r['measurement_date']}, after the capture; "
                                 "not kept")
    parsed["rows"] = [r for r in parsed["rows"] if r["measurement_date"] <= late]
    if not parsed["rows"]:
        # keep the page anyway (once per distinct content) so a layout change loses no reading
        sha = hashlib.sha256(body).hexdigest()
        if any(c["dam"] == slug and c["data_sha256"] == f"page:{sha}" for c in captures):
            summary["unchanged"] += 1
            return
        name = f"unparsed/{slug}_{stamp}__{sha[:12]}.html.gz"
        (RAW_DIR / name).parent.mkdir(parents=True, exist_ok=True)
        (RAW_DIR / name).write_bytes(gzip.compress(body, mtime=0))
        captures.append({"dam": slug, "raw_file": name, "captured_utc": captured.isoformat(), "origin": origin,
                         "data_sha256": f"page:{sha}", "columns": ";".join(parsed["columns"]), "rows": 0,
                         "first_date": "", "last_date": ""})
        summary["new_raw_pages"].append(name)
        return
    h = data_hash(parsed)
    if any(c["dam"] == slug and c["data_sha256"] == h for c in captures):
        summary["unchanged"] += 1
        return
    name = f"{captured.year}/{slug}_{stamp}__{h[:12]}.html.gz"
    (RAW_DIR / name).parent.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / name).write_bytes(gzip.compress(body, mtime=0))
    dates = [r["measurement_date"] for r in parsed["rows"]]
    captures.append({"dam": slug, "raw_file": name, "captured_utc": captured.isoformat(), "origin": origin,
                     "data_sha256": h, "columns": ";".join(parsed["columns"]), "rows": len(dates),
                     "first_date": min(dates), "last_date": max(dates)})
    summary["new_raw_pages"].append(name)


def rebuild(captures: list[dict], summary: dict) -> None:
    table: dict[tuple[str, str], dict] = {}
    revisions: list[dict] = []
    extra: set[str] = set()
    for cap in sorted(captures, key=lambda c: (c["captured_utc"], c["raw_file"])):
        if not cap["raw_file"] or cap["raw_file"].startswith("unparsed/"):
            continue
        parsed = parse_page(gzip.decompress((RAW_DIR / cap["raw_file"]).read_bytes()))
        late = (datetime.fromisoformat(cap["captured_utc"]).date() + timedelta(days=1)).isoformat()
        extra.update(c for c in parsed["columns"] if c != "measurement_date" and c not in VALUE_COLUMNS)
        for r in parsed["rows"]:
            if r["measurement_date"] > late:
                continue  # dated after its capture: reported by store(), never kept
            key = (cap["dam"], r["measurement_date"])
            row = {"dam": cap["dam"], **r, "captured_utc": cap["captured_utc"], "raw_file": cap["raw_file"]}
            old = table.get(key)
            if old:
                for f in set(old) & set(row) - {"dam", "measurement_date", "captured_utc", "raw_file"}:
                    if old[f] and row[f] and old[f] != row[f]:
                        revisions.append({"dam": key[0], "measurement_date": key[1], "field": f,
                                          "previous_value": old[f], "new_value": row[f],
                                          "previous_captured_utc": old["captured_utc"],
                                          "new_captured_utc": cap["captured_utc"], "new_raw_file": cap["raw_file"]})
                # a column the later page does not print keeps the earlier page's value
                for f in set(old) - set(row):
                    row[f] = old[f]
            table[key] = row
    columns = ["dam", "measurement_date", *VALUE_COLUMNS, *sorted(extra), "captured_utc", "raw_file"]
    rows = [{c: r.get(c, "") for c in columns} for r in sorted(table.values(),
                                                               key=lambda r: (r["dam"], r["measurement_date"]))]
    write_csv(TS_DIR / f"{TS_NAME}.csv", columns, rows)
    write_csv(TS_DIR / f"{TS_NAME}_revisions.csv", REVISION_COLUMNS,
              sorted(revisions, key=lambda r: (r["dam"], r["measurement_date"], r["new_captured_utc"], r["field"])))
    write_csv(RAW_DIR / "captures.csv", CAPTURE_COLUMNS,
              sorted(captures, key=lambda c: (c["captured_utc"], c["dam"], c["raw_file"])))
    by_dam: dict[str, list[str]] = {}
    for r in rows:
        by_dam.setdefault(r["dam"], []).append(r["measurement_date"])
    summary.update(stored_pages=len(captures), reading_rows=len(rows), revisions=len(revisions),
                   dams={k: {"readings": len(v), "first": min(v), "last": max(v)} for k, v in sorted(by_dam.items())},
                   latest_observation_date=max((r["measurement_date"] for r in rows), default=None))


def import_pages(import_dir: Path, origin: str, captures: list[dict], summary: dict) -> None:
    for p in sorted(import_dir.glob("*.html")):
        slug, _, stamp = p.stem.partition("_")
        try:
            fmt = "%Y%m%dT%H%M%S" if "T" in stamp else "%Y%m%d%H%M%S"
            captured = datetime.strptime(stamp, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            summary["errors"].append(f"{p.name}: name is not <slug>_<capture time UTC>")
            continue
        if slug not in DAMS:
            summary["errors"].append(f"{p.name}: unknown dam {slug!r}")
            continue
        store(slug, p.read_bytes(), captured, origin, captures, summary)
        summary["pages_ok"] += 1


def main() -> int:
    started = utc_now()
    stamp = started.strftime("%Y-%m-%dT%H%M%SZ")
    summary = {"source": "turkey/koski", "agency": SOURCE_AGENCY, "started_utc": started.isoformat(),
               "mode": "fetch", "http_status": {}, "pages_ok": 0, "unchanged": 0, "new_raw_pages": [],
               "errors": [], "blocked": False}
    captures = read_captures()
    import_dir = os.environ.get("IMPORT_DIR", "").strip()
    if import_dir:
        summary["mode"] = "import"
        import_pages(Path(import_dir), os.environ.get("IMPORT_ORIGIN", "").strip() or "imported",
                     captures, summary)
    else:
        for i, slug in enumerate(DAMS):
            if i:
                time.sleep(PAUSE_SECONDS)
            url = URL_TMPL.format(slug=slug)
            log(f"KOSKİ: fetching {url}")
            status, body, err = fetch(url)
            summary["http_status"][slug] = status or err
            if status == 200:
                store(slug, body, utc_now(), "live", captures, summary)
                summary["pages_ok"] += 1
            else:
                summary["errors"].append(f"{slug}: fetch failed: {err}")
                if status in BLOCKED:
                    summary["blocked"] = True
                    break
    if captures:
        rebuild(captures, summary)
    summary["finished_utc"] = utc_now().isoformat()
    summary["status"] = ("failed" if not summary["pages_ok"] or not captures
                         else "partial" if summary["errors"] else "ok")
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1) + "\n",
                                                       encoding="utf-8")
    log(json.dumps({k: v for k, v in summary.items() if k != "new_raw_pages"}, ensure_ascii=False))
    if summary["status"] == "failed":
        log("ERROR: no KOSKİ page could be retrieved in this run.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

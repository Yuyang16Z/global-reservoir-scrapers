"""NWC (Jamaica) - weekly storage of Mona Reservoir and Hermitage Dam on "Dam & Reservoir Levels".

Source:
  https://www.nwcjamaica.com/reservoir.php

The National Water Commission's page shows, for Mona Reservoir and Hermitage Dam (the two main storage sources of the
Kingston & St Andrew supply), the LATEST weekly reading only: the stored volume in million imperial gallons (MG), the
percentage of capacity, the capacity (MG and megalitres), the reading date ("As of: 01 Oct 26"), a status label and
the trend against the previous reading. The page calls the readings weekly; NWC posted readings dated 1, 4 and 5
October 2026, so a reading can be replaced within a day.

Retention: CURRENT SNAPSHOT. Each reading replaces the previous one and nothing on the page or elsewhere on the site
serves earlier readings; history survives only in Internet Archive captures (30 distinct, 2021-08 to 2026-07) and
press releases. Two layouts are read: the 2026 cards and the earlier one-<section>-per-reservoir "Weekly Summary"
page. Archived captures can be imported with IMPORT_DIR.

Design:
- Each run fetches the page once. A page is stored only when its parsed readings are new (hash of the parsed readings,
  not of the HTML), so the twice-daily runs store about one page a week.
- A page the parser cannot read is still stored, under raw/unparsed/ (once per distinct content), and the run ends
  as "partial", so a layout change loses no reading and is visible in the run log.
- The CSV is rebuilt from every stored page on every run, keyed on (reservoir, observation_date) where
  observation_date is the page's own "As of" date, never the fetch time; a later capture wins and a value that
  changes is listed in the revisions CSV. Reruns add nothing.

Licence: NWC's copyright notice (https://www.nwcjamaica.com/copyright.php) allows personal and non-commercial public use
and reproduction without charge or further permission, provided the copy is accurate, NWC is identified as the source
and the copy is not presented as an official version or as endorsed by NWC; reproduction for commercial
redistribution needs NWC's written permission. Collected here by owner decision of 2026-10-04 (same route as
argentina/cammesa_hidro and costarica/cence_embalses); see DATA_LICENCE_NOTICE.md.

Environment:
  OUTPUT_DIR     output root (default <script_dir>/outputs)
  IMPORT_DIR     optional directory of saved pages named <capture_utc>.html (e.g. Internet Archive timestamps
                 20240908022057.html) to add to the archive; the page itself is not fetched when set
  IMPORT_ORIGIN  label recorded for imported pages (default "imported"; "internet_archive" for Wayback copies)

Outputs (under OUTPUT_DIR):
  raw/<YYYY>/<capturedUTC>__<data12>.html.gz      one stored page per distinct set of readings
  raw/unparsed/<capturedUTC>__<sha12>.html.gz     pages the parser could not read (once per distinct content)
  raw/captures.csv                                every stored page: capture time, origin, data hash, layout, dates
  timeseries/jamaica_nwc_reservoirs.csv           reservoir, observation_date, storage_mg, storage_pct, capacity_mg,
                                                  capacity_ml, status_label, trend, captured_utc, raw_file
  timeseries/jamaica_nwc_reservoirs_revisions.csv
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

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

URL = "https://www.nwcjamaica.com/reservoir.php"
SOURCE_AGENCY = "National Water Commission (NWC), Jamaica"
TIMEOUT = 60
REQUEST_BACKOFFS = (10, 30, 90)
HEADERS = {"User-Agent": ("Mozilla/5.0 (compatible; global-reservoir-scrapers/1.0; "
                          "+https://github.com/Yuyang16Z/global-reservoir-scrapers)")}
DATE_FORMATS = ("%d %b %y", "%d %b %Y", "%d %B %Y", "%d %B %y", "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y",
                "%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y")
NUM_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")
READING_COLUMNS = ["reservoir", "observation_date", "storage_mg", "storage_pct", "capacity_mg", "capacity_ml",
                   "status_label", "trend", "captured_utc", "raw_file"]
REVISION_COLUMNS = ["reservoir", "observation_date", "field", "previous_value", "new_value",
                    "previous_captured_utc", "new_captured_utc", "new_raw_file"]
CAPTURE_COLUMNS = ["raw_file", "captured_utc", "origin", "data_sha256", "layout", "as_of_dates"]
VALUE_FIELDS = ("storage_mg", "storage_pct", "capacity_mg", "capacity_ml")


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def number(text: str | None) -> float | None:
    m = NUM_RE.search(text or "")
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def parse_date(text: str | None) -> str | None:
    t = re.sub(r"\s+", " ", (text or "").strip().rstrip("."))
    t = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", t)
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def unit_value(text: str, unit: str) -> float | None:
    """The number written before a unit ('808.5 MG / 3,675 ML', unit 'ML' -> 3675.0)."""
    m = re.search(r"(-?\d[\d,]*(?:\.\d+)?)\s*" + unit + r"\b", text or "")
    return float(m.group(1).replace(",", "")) if m else None


def parse_cards(soup: BeautifulSoup) -> list[dict]:
    """Layout of 2026: one 'res-card' per reservoir with a 'res-meta' line 'Capacity / Reading / As of'."""
    readings = []
    for card in soup.select("div.res-card"):
        head = card.find(["h2", "h3"])
        if not head:
            continue
        meta = {}
        for span in card.select(".res-meta span"):
            label, _, value = span.get_text(" ", strip=True).partition(":")
            meta[label.strip().lower()] = value.strip()
        pct_el = card.select_one(".g-pct")
        mg_el = card.select_one(".g-mg")
        status_el = card.select_one(".status-pill")
        trend_el = card.select_one(".g-trend")
        capacity = meta.get("capacity", "")
        readings.append({
            "reservoir": head.get_text(" ", strip=True),
            "observation_date": parse_date(meta.get("as of")),
            "as_of_printed": meta.get("as of", ""),
            "storage_mg": unit_value(meta.get("reading", ""), "MG") if meta.get("reading") else
            number(mg_el.get_text(" ", strip=True) if mg_el else None),
            "storage_pct": number(pct_el.get_text(" ", strip=True) if pct_el else None),
            "capacity_mg": unit_value(capacity, "MG"),
            "capacity_ml": unit_value(capacity, "ML"),
            "status_label": status_el.get_text(" ", strip=True) if status_el else "",
            "trend": trend_el.get_text(" ", strip=True) if trend_el else "",
        })
    return readings


LEGACY_CAPACITY_RE = re.compile(r"([A-Z][A-Za-z .]*?)\s+Capacity\s*:?\s*(-?\d[\d,]*(?:\.\d+)?)\s*MG\s*/\s*"
                                r"(-?\d[\d,]*(?:\.\d+)?)\s*s?\s*ML", re.I)
LEGACY_DATE_RE = re.compile(r"Last\s+Updated(?:\s+Reading)?\s*:?\s*([A-Za-z]{3,9}\.?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|"
                            r"\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\.?,?\s+\d{2,4})", re.I)
LEGACY_MG_RE = re.compile(r"(-?\d[\d,]*(?:\.\d+)?)\s*MG\s*Reading", re.I)
LEGACY_PCT_RE = re.compile(r"(-?\d[\d,]*(?:\.\d+)?)\s*%\s*Level\s+Percentage", re.I)


def parse_sections(soup: BeautifulSoup) -> list[dict]:
    """Layout to mid-2026: one <section> per reservoir, 'Reservoir Levels | Weekly Summary', '<name> Capacity
    808.5 MG/3,675 ML', 'Last Updated Reading Jul 08, 2026', '642.7 MG Reading per Million Gallons', '79.5 % Level
    Percentage' and an up or down arrow image. HTML comments (an older static block) are not read."""
    readings = []
    for sec in soup.find_all("section"):
        text = re.sub(r"\s+", " ", sec.get_text(" ", strip=True))
        cap = LEGACY_CAPACITY_RE.search(text)
        if not cap or "Reading" not in text:
            continue
        date_m = LEGACY_DATE_RE.search(text)
        mg = LEGACY_MG_RE.search(text)
        pct = LEGACY_PCT_RE.search(text)
        arrow = next((img.get("alt", "") for img in sec.find_all("img") if "arrow" in (img.get("alt", "") +
                                                                                    img.get("src", "")).lower()), "")
        # the name is the text in front of 'Capacity' in its own heading ('Mona Reservoir Capacity <strong>...')
        node = sec.find(string=re.compile(r"Capacity", re.I))
        name = re.split(r"(?i)\s*Capacity", str(node))[0].strip() if node else cap.group(1).strip()
        readings.append({
            "reservoir": re.sub(r"\s+Levels?$", "", re.sub(r"\s+", " ", name)),
            "observation_date": parse_date(date_m.group(1)) if date_m else None,
            "as_of_printed": date_m.group(1) if date_m else "",
            "storage_mg": float(mg.group(1).replace(",", "")) if mg else None,
            "storage_pct": float(pct.group(1).replace(",", "")) if pct else None,
            "capacity_mg": float(cap.group(2).replace(",", "")),
            "capacity_ml": float(cap.group(3).replace(",", "")),
            "status_label": "",
            "trend": arrow,
        })
    return readings


def parse_page(body: bytes) -> dict:
    """Readings of one page: {layout, readings: [{reservoir, observation_date, storage_mg, storage_pct, capacity_mg,
    capacity_ml, status_label, trend}], problems: [...]}. A reading without a date or a volume is reported, not kept."""
    soup = BeautifulSoup(body.decode("utf-8", errors="replace"), "html.parser")
    layout, readings = "", []
    if soup.select("div.res-card"):
        layout, readings = "res_card_2026", parse_cards(soup)
    else:
        readings = parse_sections(soup)
        layout = "weekly_summary_sections" if readings else ""
    problems, kept = [], []
    for r in readings:
        if not r["observation_date"]:
            problems.append(f"{r['reservoir']}: 'As of' date not readable ({r.get('as_of_printed')!r})")
        elif r["storage_mg"] is None and r["storage_pct"] is None:
            problems.append(f"{r['reservoir']}: no volume or percentage")
        else:
            kept.append({k: v for k, v in r.items() if k != "as_of_printed"})
    if not layout:
        problems.append("no reservoir card or 'Weekly Summary' section found (layout not recognised)")
    return {"layout": layout, "readings": kept, "problems": problems}


def data_hash(parsed: dict) -> str:
    return hashlib.sha256(json.dumps(sorted(parsed["readings"], key=lambda r: r["reservoir"]), sort_keys=True,
                                     ensure_ascii=False).encode("utf-8")).hexdigest()


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
    stamp = captured.strftime("%Y%m%dT%H%M%SZ")
    for p in parsed["problems"]:
        summary["errors"].append(f"{origin} {captured.isoformat(timespec='seconds')}: {p}")
    future = [r for r in parsed["readings"]
              if date.fromisoformat(r["observation_date"]) > captured.date() + timedelta(days=1)]
    for r in future:
        summary["errors"].append(f"{origin} {stamp}: {r['reservoir']} dated {r['observation_date']}, after the "
                                 "capture; reading not kept")
    parsed["readings"] = [r for r in parsed["readings"] if r not in future]
    if not parsed["readings"]:
        # keep the page anyway (once per distinct content) so a layout change loses no reading
        sha = hashlib.sha256(body).hexdigest()
        if any(c["data_sha256"] == f"page:{sha}" for c in captures):
            summary["unchanged"] += 1
            return
        name = f"unparsed/{stamp}__{sha[:12]}.html.gz"
        (RAW_DIR / name).parent.mkdir(parents=True, exist_ok=True)
        (RAW_DIR / name).write_bytes(gzip.compress(body, mtime=0))
        captures.append({"raw_file": name, "captured_utc": captured.isoformat(timespec="seconds"), "origin": origin,
                         "data_sha256": f"page:{sha}", "layout": "unparsed", "as_of_dates": ""})
        summary["new_raw_pages"].append(name)
        return
    h = data_hash(parsed)
    if any(c["data_sha256"] == h for c in captures):
        summary["unchanged"] += 1
        return
    name = f"{captured.year}/{stamp}__{h[:12]}.html.gz"
    (RAW_DIR / name).parent.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / name).write_bytes(gzip.compress(body, mtime=0))
    captures.append({"raw_file": name, "captured_utc": captured.isoformat(timespec="seconds"), "origin": origin,
                     "data_sha256": h, "layout": parsed["layout"],
                     "as_of_dates": ";".join(sorted({r["observation_date"] for r in parsed["readings"]}))})
    summary["new_raw_pages"].append(name)


def fmt(v) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, float):
        return f"{v:.3f}".rstrip("0").rstrip(".")
    return str(v)


def rebuild(captures: list[dict], summary: dict) -> None:
    table: dict[tuple[str, str], dict] = {}
    revisions = []
    for cap in sorted(captures, key=lambda c: (c["captured_utc"], c["raw_file"])):
        if cap["layout"] == "unparsed":
            continue
        parsed = parse_page(gzip.decompress((RAW_DIR / cap["raw_file"]).read_bytes()))
        latest_ok = (datetime.fromisoformat(cap["captured_utc"]).date() + timedelta(days=1)).isoformat()
        for r in parsed["readings"]:
            if r["observation_date"] > latest_ok:
                continue  # dated after its capture: reported by store(), never kept
            key = (r["reservoir"], r["observation_date"])
            row = {"reservoir": r["reservoir"], "observation_date": r["observation_date"],
                   **{f: fmt(r.get(f)) for f in (*VALUE_FIELDS, "status_label", "trend")},
                   "captured_utc": cap["captured_utc"], "raw_file": cap["raw_file"]}
            old = table.get(key)
            if old:
                for f in VALUE_FIELDS:
                    if old[f] and row[f] and old[f] != row[f]:
                        revisions.append({"reservoir": key[0], "observation_date": key[1], "field": f,
                                          "previous_value": old[f], "new_value": row[f],
                                          "previous_captured_utc": old["captured_utc"],
                                          "new_captured_utc": cap["captured_utc"], "new_raw_file": cap["raw_file"]})
            table[key] = row
    rows = sorted(table.values(), key=lambda r: (r["reservoir"], r["observation_date"]))
    write_csv(TS_DIR / "jamaica_nwc_reservoirs.csv", READING_COLUMNS, rows)
    write_csv(TS_DIR / "jamaica_nwc_reservoirs_revisions.csv", REVISION_COLUMNS, revisions)
    write_csv(RAW_DIR / "captures.csv", CAPTURE_COLUMNS, sorted(captures, key=lambda c: c["captured_utc"]))
    by_res = {}
    for r in rows:
        by_res.setdefault(r["reservoir"], []).append(r["observation_date"])
    summary.update(stored_pages=len(captures), reading_rows=len(rows), revisions=len(revisions),
                   reservoirs={k: {"readings": len(v), "first": min(v), "last": max(v)} for k, v in sorted(by_res.items())},
                   latest_observation_date=max((r["observation_date"] for r in rows), default=None))


def main() -> int:
    started = utc_now()
    stamp = started.strftime("%Y-%m-%dT%H%M%SZ")
    summary = {"source": "jamaica/nwc_reservoirs", "agency": SOURCE_AGENCY, "url": URL,
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
        log(f"NWC: fetching {URL}")
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
        log("ERROR: the page could not be retrieved in this run.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

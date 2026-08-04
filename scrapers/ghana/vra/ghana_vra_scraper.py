"""Ghana VRA Akosombo (Lake Volta) level scraper.

Source:
- https://vra.com/resources/facts.php   Volta River Authority facts page
- https://vra.com/                      homepage widget (same reading)

Known quirks:
- Use the NON-www host. `www.vra.com` redirects to a broken
  `~4008842.../htdocs/` path.
- The page is a CURRENT-VALUE SNAPSHOT with no archive, so anything not
  captured is lost; only continuous polling builds a series.
- The reading is printed together with its OWN observation date, which lags
  the page by 0-5 days. The observation date is always the printed date,
  never the fetch date. Two layouts occur:
      "Lake Level 261.48 feet 79.699 meters Thursday, February 27, 2020"
      "Lake Level (Friday, September 20, 2024): 264.15 feet | 80.512 meters"
  and facts.php prints the date BEFORE the values, sometimes followed by the
  same-date reading one year earlier - that second pair is a real observation
  for its own date and is captured too.
- VRA publishes the same reading in feet and metres; both are stored as
  published (no project conversion).

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/facts_<stamp>.html, raw/home_<stamp>.html
  timeseries/ghana_vra_akosombo.csv   (merged by observation date, idempotent)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import html as html_mod
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

FACTS_URL = "https://vra.com/resources/facts.php"
HOME_URL = "https://vra.com/"
RESERVOIR_ID = "GH_VRA_AKOSOMBO"
TIMEOUT = 120
RETRIES = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

DATE_TXT = (r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?,?\s*"
            r"[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}")
BLOCK_RE = re.compile(
    r"Lake\s*Level\s*(?:\((?P<d1>[^)]{6,40})\)\s*:?)?\s*"
    r"(?P<ft>\d{2,3}\.\d{1,3})\s*(?:feet|ft)\s*\|?\s*"
    r"(?P<m>\d{2,3}\.\d{1,3})\s*(?:meters|metres|m)\b"
    r"(?:[\s,]*(?P<d2>" + DATE_TXT + r"))?", re.I)
PAIR_RE = re.compile(
    r"(?P<d>" + DATE_TXT + r")\s+"
    r"(?P<ft>\d{2,3}\.\d{1,3})\s*(?:feet|ft)\s*\|?\s*"
    r"(?P<m>\d{2,3}\.\d{1,3})\s*(?:meters|metres|m)\b", re.I)

COLUMNS = ["measurement_date", "water_level_masl", "water_level_ft",
           "reservoir_id", "source_page", "fetched_at"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def get_with_retries(url: str):
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        except requests.RequestException as exc:
            print(f"[WARN] {url} attempt {attempt}/{RETRIES}: {exc!r}", flush=True)
            time.sleep(10 * attempt)
            continue
        if r.status_code == 200 and len(r.content) > 1500:
            return r
        print(f"[WARN] {url} attempt {attempt}/{RETRIES}: "
              f"HTTP {r.status_code}, {len(r.content)}B", flush=True)
        time.sleep(10 * attempt)
    return None


def page_text(raw: bytes) -> str:
    t = raw.decode("utf-8", "replace")
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", t, flags=re.S)
    t = html_mod.unescape(re.sub(r"<[^>]+>", " ", t))
    return re.sub(r"\s+", " ", t)


MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], 1)}
# VRA always prints "[Weekday, ]Month D, YYYY"; parsed with the standard library
# so the scraper adds no dependency beyond the repo's existing requirements.
ISO_RE = re.compile(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})\s*$")


def iso(raw: str) -> str | None:
    m = ISO_RE.search(raw.strip(" ,"))
    if not m:
        return None
    month = MONTHS.get(m.group(1).lower())
    if not month:
        return None
    day, year = int(m.group(2)), int(m.group(3))
    if not (1 <= day <= 31 and 2000 <= year <= 2100):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_readings(text: str, page: str, fetched_at: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    m = BLOCK_RE.search(text)
    if m:
        d = iso(m.group("d1") or m.group("d2") or "")
        if d:
            out.append({"measurement_date": d, "water_level_masl": m.group("m"),
                        "water_level_ft": m.group("ft"),
                        "reservoir_id": RESERVOIR_ID, "source_page": page,
                        "fetched_at": fetched_at})
            seen.add(d)
    idx = text.lower().find("lake level")
    if idx >= 0:
        for pm in PAIR_RE.finditer(text[idx:idx + 600]):
            d = iso(pm.group("d"))
            if d and d not in seen:
                out.append({"measurement_date": d,
                            "water_level_masl": pm.group("m"),
                            "water_level_ft": pm.group("ft"),
                            "reservoir_id": RESERVOIR_ID, "source_page": page,
                            "fetched_at": fetched_at})
                seen.add(d)
    return out


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
        if key not in existing:
            added += 1
        else:
            old = existing[key]
            compare = [c for c in columns if c not in ("fetched_at", "source_page")]
            if {c: old[c] for c in compare} == {c: norm[c] for c in compare}:
                continue
            updated += 1
        existing[key] = norm
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for key in sorted(existing):
            w.writerow(existing[key])
    print(f"[SAVE] {path} total={len(existing)} added={added} updated={updated}",
          flush=True)
    return added, updated


def append_run_log(log: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = RUN_LOG_DIR / f"{utc_stamp()}_summary.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {path}", flush=True)


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    stamp = utc_stamp()
    log = {"started_at": fetched_at, "source": "ghana/vra",
           "urls": [FACTS_URL, HOME_URL], "output_dir": str(OUTPUT_DIR),
           "status": "started", "errors": []}
    try:
        rows: list[dict] = []
        pages_ok = 0
        for url, slug in ((FACTS_URL, "facts"), (HOME_URL, "home")):
            r = get_with_retries(url)
            if r is None:
                log.setdefault("unavailable_pages", []).append(slug)
                continue
            pages_ok += 1
            raw_path = RAW_DIR / f"{slug}_{stamp}.html"
            raw_path.write_bytes(r.content)
            print(f"[SAVE] {raw_path} ({len(r.content)}B)", flush=True)
            found = parse_readings(page_text(r.content), slug, fetched_at)
            print(f"[INFO] {slug}: {len(found)} reading(s)", flush=True)
            rows.extend(found)
        if pages_ok == 0:
            raise RuntimeError("both VRA pages unavailable after retries")
        if not rows:
            # the widget can be briefly blank; keep the raw page and report
            log["status"] = "no_reading_found"
            log["finished_at"] = utc_now_iso()
            append_run_log(log)
            print("[WARN] no lake-level reading parsed this run", flush=True)
            return 0
        added, updated = merge_csv(TS_DIR / "ghana_vra_akosombo.csv",
                                   COLUMNS, rows, ["measurement_date"])
        dates = sorted(r["measurement_date"] for r in rows)
        log.update({"status": "ok", "readings": len(rows),
                    "date_range": [dates[0], dates[-1]],
                    "rows_added": added, "rows_updated": updated,
                    "finished_at": utc_now_iso()})
        append_run_log(log)
        return 0
    except Exception as exc:  # noqa: BLE001 - scheduled job: log and signal
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

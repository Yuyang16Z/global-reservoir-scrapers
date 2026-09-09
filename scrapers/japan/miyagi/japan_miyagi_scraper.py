#!/usr/bin/env python3
"""Miyagi Prefecture dams — monthly storage-status PDF.

18 prefecture-managed dams with reservoir level (m above sea level) and storage as a
percentage of USABLE capacity. Published as a single PDF whose filename carries the
Reiwa-era date, e.g. `r80901_tyosui.pdf` = 令和8年9月1日.

RETENTION: `overwrite_prone`. Older dated filenames 404 — verified 2026-09-09 for
r80801 / r80701 / r80601 / r80815 — so the site keeps only the current issue, and the
Internet Archive holds just three snapshots under three different naming schemes
(damutyosuii.pdf 2022, tyosui.pdf 2024, r7_7tyosuii.pdf 2025), which is not a usable
backfill. A missed issue is lost.

The PDF link is read from the landing page rather than constructed from a guessed
date, because the filename convention has changed at least twice.

EXCLUSIONS
  * 過去10年平均の貯水位 / 過去10年平均の貯水率 — ten-year climatological means, not
    observations of this date.
  * 水位順位 — a rank, not a measurement.
  * The 全体 row — an all-dam aggregate, not a water body.

storage_pct_usable, not storage_pct: the sheet states the denominator as 利水容量
(現在の貯水量／利水容量).
"""
from __future__ import annotations

import csv
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber

BASE_DIR = Path(__file__).resolve().parent
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
RAW = OUT / "raw"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
LANDING = "https://www.pref.miyagi.jp/soshiki/kasen/dam-cyosuiiall.html"
ROOT = "https://www.pref.miyagi.jp"
COLS = ["dam_jp", "measurement_date", "observed_time",
        "water_level_masl", "storage_pct_usable"]
# 令和1 = 2019
REIWA_BASE = 2018
DATE_RE = re.compile(r"令和\s*(\d+)\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2})?\s*時?")
NAME_RE = re.compile(r"^[ぁ-んァ-ヶ一-龥]{1,8}$")
NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
AGGREGATE = {"全体", "合計", "計"}


def get(url: str, attempts: int = 3) -> bytes | None:
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except (urllib.error.URLError, OSError, TimeoutError):
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
    return None


def find_pdf() -> str | None:
    raw = get(LANDING)
    if raw is None:
        return None
    for enc in ("utf-8", "shift_jis", "euc_jp"):
        try:
            html = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        html = raw.decode("utf-8", errors="replace")
    for href in re.findall(r'href="([^"]+\.pdf)"', html):
        if "tyosui" in href.lower():
            return href if href.startswith("http") else ROOT + href
    return None


def parse(pdf_path: Path) -> tuple[list[dict], str | None, str | None]:
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""
        tables = page.extract_tables()
    m = DATE_RE.search(text)
    if not m:
        return [], None, None
    year = REIWA_BASE + int(m.group(1))
    date = f"{year:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    obs_time = f"{int(m.group(4)):02d}:00" if m.group(4) else ""

    rows = []
    for tb in tables:
        for r in tb:
            if not r or not r[0]:
                continue
            name = str(r[0]).strip()
            if not NAME_RE.match(name) or name in AGGREGATE:
                continue
            # column 1 = 貯水位(標高m), column 2 = 貯水率(%). Columns 3 and 4 are
            # ten-year climatological means and column 5 is a rank; all excluded.
            lvl = str(r[1]).strip() if len(r) > 1 and r[1] else ""
            pct = str(r[2]).strip() if len(r) > 2 and r[2] else ""
            if not (NUM_RE.match(lvl) or NUM_RE.match(pct)):
                continue
            rec = {"dam_jp": name, "measurement_date": date, "observed_time": obs_time}
            if NUM_RE.match(lvl):
                rec["water_level_masl"] = float(lvl)
            if NUM_RE.match(pct):
                rec["storage_pct_usable"] = float(pct)
            rows.append(rec)
        if rows:
            break
    return rows, date, obs_time


def main() -> int:
    url = find_pdf()
    if url is None:
        print("no tyosui PDF link found on the landing page")
        return 1
    raw = get(url)
    if raw is None:
        print(f"download failed: {url}")
        return 1
    RAW.mkdir(parents=True, exist_ok=True)
    name = url.rsplit("/", 1)[-1]
    path = RAW / name
    path.write_bytes(raw)

    rows, date, obs_time = parse(path)
    if not rows:
        print(f"no dam rows parsed from {name}")
        return 1

    OUT.mkdir(parents=True, exist_ok=True)
    acc = OUT / "accumulated.csv"
    seen = set()
    if acc.exists():
        with acc.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                seen.add((r["dam_jp"], r["measurement_date"]))
    new = [r for r in rows if (r["dam_jp"], r["measurement_date"]) not in seen]
    write_header = not acc.exists()
    with acc.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        if write_header:
            w.writeheader()
        for r in sorted(new, key=lambda r: r["dam_jp"]):
            w.writerow({c: r.get(c, "") for c in COLS})
    (OUT / "runlog_latest.json").write_text(json.dumps(
        {"run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "pdf": name, "url": url, "bulletin_date": date, "observed_time": obs_time,
         "parsed": len(rows), "appended": len(new)},
        ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  {name}  {date} {obs_time}  解析 {len(rows)} 坝, 累积新增 {len(new)}")
    for r in rows[:4]:
        print(f"    {r['dam_jp']:<8} 水位={r.get('water_level_masl')} 蓄率={r.get('storage_pct_usable')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Gunma Prefecture dams — 群馬県水位雨量情報システム hourly mobile pages.

7 prefectural dams (坂本 霧積 塩沢 四万川 道平川 大仁田 桐生川), each with all four
priority variables on an hourly 25-point rolling window.

HOW THE ENTRY POINT WAS FOUND. The desktop site (www.river-gunma.jp/gunma/top/...)
renders dam readings only into <img alt=""> text with no observation timestamp, and
its navigation is built by riverMap.js at runtime, so no dam URL is reachable by
reading the landing HTML. The site's own mobile edition, linked from the desktop
page, publishes plain Shift_JIS pages with a stable path scheme. Every URL below
came from a link on a fetched page — none was guessed:

    /k/                        -> ダム情報  /gunmaT/m4001/10/_0_1_0.html
    /gunmaT/m4001/...          -> one link per dam, index 1..7
    /gunmaT/m4101/{10,60}/...  -> 現況表: all four variables, LATEST VALUE ONLY
    /gunmaT/m4102/{10,60}/...  -> 貯水位   time series
    /gunmaT/m4103/{10,60}/...  -> 流入量   time series
    /gunmaT/m4104/{10,60}/...  -> 放流量   time series
    /gunmaT/m4110/{10,60}/...  -> 貯水量   time series

The 現況表 carries no history, so the four per-variable pages are fetched instead:
7 dams x 4 variables = 28 requests, each returning 25 hourly points.

/60/ (hourly, 25 points = 24h) is used rather than /10/ (10-minute, 25 points =
4h) because the window has to survive a once-daily run.

TWO GUARDS, both against errors this project has actually made before:

  - The dam is identified by the name PRINTED ON THE PAGE, never by the index in
    the URL. If the site reorders its dam list, rows follow the name.
  - Each page declares its own variable in the header (ダム貯水位 / 流入量(時間) /
    ダム放流量 / ダム貯水量). That declaration is ASSERTED against the variable the
    URL was fetched for; a mismatch drops the page and is reported. This is the
    same header-label discipline the servlet-family scraper needed after positional
    column mapping was found writing 流入量 into water_level_m.

UNITS as declared by each page: 貯水位 m (masl, dam crest datum), 流入量/放流量 m3/s,
貯水量 千m3 -> x0.001 -> mcm.

Values carry a trend arrow in the same cell (520.73→) which is stripped.
"""
from __future__ import annotations

import csv
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
RAW = OUT / "raw"
ROOT = "http://www.river-gunma.jp"
UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")
JST = timezone(timedelta(hours=9))
PACE = 1.5

# page code -> (delivered variable, unit factor, header text the page must declare)
VARIABLES = {
    "m4102": ("water_level_masl", 1.0, "貯水位"),
    "m4103": ("total_inflow_m3s", 1.0, "流入量"),
    "m4104": ("total_outflow_m3s", 1.0, "放流量"),
    "m4110": ("storage_mcm", 0.001, "貯水量"),
}
DAM_INDEX = range(1, 8)
MISSING = {"", "-", "－", "ー", "欠測", "未収集", "*", "***"}
COLS = ["dam_jp", "observed_at", "water_level_masl", "storage_mcm",
        "total_inflow_m3s", "total_outflow_m3s"]

# Two markup variants ship from the same system, so rows are read cell-by-cell
# rather than by a fixed cell pattern:
#   m4102/m4104/m4110 -> <TD>09/09</TD><TD>09:00</TD><TD>520.73→</TD>
#   m4103             -> <TD>09/09 09:00</TD><TD>5.60</TD><TD>↑</TD>
# A value must fill its whole cell (optionally with a trend arrow). That anchoring
# is what keeps the combined "09/09 09:00" cell from being read as a number: strip
# its punctuation and it becomes 09090900, which float() accepts.
CELL = re.compile(r"<TD[^>]*>(.*?)</TD>", re.I | re.S)
VALUE = re.compile(r"^(-?[\d,]+(?:\.\d+)?)\s*[→↑↓]?$")
DATE = re.compile(r"(\d{1,2}/\d{1,2})")
TIME = re.compile(r"(\d{1,2}:\d{2})")
DAM_NAME = re.compile(r"([^\s<>]+ダム)")
HEADER = re.compile(r"ダム情報\s*(.{0,24}?)\s*−緊急情報−", re.S)


def get(url: str, attempts: int = 3) -> str | None:
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": ROOT + "/k/"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read().decode("shift_jis", errors="replace")
        except (urllib.error.URLError, OSError, TimeoutError):
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
    return None


def num(raw: str, factor: float) -> float | None:
    s = re.sub(r"[^0-9.\-]", "", str(raw))          # drops → ↑ ↓ and thousands commas
    if not s or s in MISSING:
        return None
    try:
        return round(float(s) * factor, 6)
    except ValueError:
        return None


def resolve_year(md: str, now: datetime) -> str | None:
    """MM/DD -> YYYY-MM-DD. The window is <=25h, so a date that would land in the
    future belongs to last year (31 Dec -> 1 Jan rollover)."""
    try:
        mo, d = (int(x) for x in md.split("/"))
    except ValueError:
        return None
    for year in (now.year, now.year - 1):
        try:
            cand = datetime(year, mo, d, tzinfo=JST)
        except ValueError:
            continue
        if cand <= now + timedelta(days=1):
            return cand.strftime("%Y-%m-%d")
    return None


def parse(html: str, code: str, now: datetime) -> tuple[str, list[tuple[str, float]], str]:
    """-> (dam name, [(observed_at, value)], note). Empty name/list means unusable."""
    var, factor, want = VARIABLES[code]
    plain = re.sub(r"<[^>]+>", " ", html)

    head = HEADER.search(plain)
    declared = re.sub(r"\s+", "", head.group(1)) if head else ""
    if want not in declared:
        return "", [], f"header mismatch: page declares {declared!r}, expected {want!r}"

    tail = plain.split("−緊急情報−", 1)[-1]
    m = DAM_NAME.search(tail)
    if not m:
        return "", [], "no dam name on page"
    dam = m.group(1)

    out, date = [], None
    for chunk in re.split(r"<TR", html, flags=re.I)[1:]:
        cells = [re.sub(r"<[^>]+>", "", c).replace("\u00a0", " ").strip()
                 for c in CELL.findall(chunk)]
        joined = " ".join(cells)
        md = DATE.search(joined)
        if md:
            date = resolve_year(md.group(1), now)
        tm = TIME.search(joined)
        if not date or not tm:
            continue
        for cell in cells:
            hit = VALUE.match(cell)
            if hit:
                v = num(hit.group(1), factor)
                if v is not None:
                    out.append((f"{date}T{tm.group(1).zfill(5)}", v))
                break
    if not out:
        return dam, [], "parsed 0 points (markup changed?)"
    return dam, out, ""


def main() -> int:
    now = datetime.now(JST)
    RAW.mkdir(parents=True, exist_ok=True)
    merged: dict[tuple[str, str], dict] = {}
    problems: list[str] = []
    dams: dict[int, str] = {}

    for idx in DAM_INDEX:
        for code, (var, _f, _w) in VARIABLES.items():
            url = f"{ROOT}/gunmaT/{code}/60/_0_{idx}_0.html"
            html = get(url)
            time.sleep(PACE)
            if html is None:
                problems.append(f"{idx}/{code}: fetch failed")
                continue
            (RAW / f"{code}_{idx}.html").write_text(html, encoding="utf-8")
            dam, points, note = parse(html, code, now)
            if note:
                problems.append(f"{idx}/{code}: {note}")
                continue
            prev = dams.setdefault(idx, dam)
            if prev != dam:
                problems.append(f"{idx}/{code}: dam name changed {prev} -> {dam}")
                continue
            for stamp, value in points:
                merged.setdefault((dam, stamp), {"dam_jp": dam, "observed_at": stamp})[var] = value

    rows = [merged[k] for k in sorted(merged)]
    if not rows:
        print("no rows parsed; " + "; ".join(problems[:6]))
        return 1

    OUT.mkdir(parents=True, exist_ok=True)
    acc = OUT / "accumulated.csv"
    seen = set()
    if acc.exists():
        with acc.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                seen.add((r["dam_jp"], r["observed_at"]))
    new = [r for r in rows if (r["dam_jp"], r["observed_at"]) not in seen]
    write_header = not acc.exists()
    with acc.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        if write_header:
            w.writeheader()
        for r in new:
            w.writerow({c: r.get(c, "") for c in COLS})

    stamps = sorted({r["observed_at"] for r in rows})
    (OUT / "runlog_latest.json").write_text(json.dumps(
        {"run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "dams": sorted(set(dams.values())), "timestamps": len(stamps),
         "first": stamps[0], "last": stamps[-1],
         "parsed": len(rows), "appended": len(new), "problems": problems},
        ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  {len(set(dams.values()))} 坝 × {len(stamps)} 时刻 "
          f"({stamps[0]} → {stamps[-1]}) = {len(rows)} 行, 累积新增 {len(new)}")
    if problems:
        print("  问题: " + "; ".join(problems[:6]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

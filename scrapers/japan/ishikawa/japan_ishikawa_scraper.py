#!/usr/bin/env python3
"""Ishikawa Prefecture dams — 石川県河川総合情報システム hourly JSON.

12 dams with an intra-day hourly series, same vendor platform as Shimane but a
different file layout and JSON shape:

    https://kasen.pref.ishikawa.lg.jp/dyn/dps/timeline/<YYYYMMDD>/<YYYYMMDD>_1_dam_60.json

    {"<station_id>": {"data60": [{"item_10": {"val": ..}, ..., "time": "..."}]},
     "updateTime": ..., "observationTime": ...}

The day file resets at midnight and past dates 404, so the late-day run is primary and
the earlier ones are insurance.

FIELD CODES share Shimane's numbering, but that was VERIFIED against this site's own
rendered ダム諸量概況図 rather than assumed from the sibling:

    item_10  貯水位 [EL.m]    手取川 447.09 (08:00) vs rendered 447.12 (08:50)
    item_50  流入量 [m3/s]    手取川 51.02 vs rendered 51.02
    item_70  全放流量 [m3/s]   手取川 0.49 vs rendered 0.49
    item_20  貯水量 [1000 m3] not shown on the overview page; carried as storage_mcm
                              on the strength of the shared code and a plausible
                              magnitude (手取川 107,752 -> 107.75 mcm against a
                              published capacity of about 231 mcm)

NOTE ON 手取川ダム: the site labels it 手取川ダム(国) — nationally managed. It may
therefore also appear in the MLIT/OpenGov delivery and must be de-duplicated there
rather than admitted twice.

Values arrive as strings with thousands separators ("107,752") and missing readings as
"-", so both are handled explicitly.
"""
from __future__ import annotations

import csv
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
RAW = OUT / "raw"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
ROOT = "https://kasen.pref.ishikawa.lg.jp"
JST = timezone(timedelta(hours=9))

FIELDS = {
    "item_10": ("water_level_masl", 1.0),
    "item_50": ("total_inflow_m3s", 1.0),
    "item_70": ("total_outflow_m3s", 1.0),
    "item_20": ("storage_mcm", 0.001),
}
MISSING = {"-", "", "－", "欠測", "未収集"}
# id -> name, derived by matching each id's item_10 against the rendered
# ダム諸量概況図 at a shared timestamp: 12/12 unique, zero ambiguity.
STATIONS = {
    "4354_7_1": "我谷ダム", "4354_7_2": "九谷ダム", "4356_7_21": "大日川ダム",
    "4361_7_31": "北河内ダム", "4361_7_41": "八ヶ川ダム", "4362_7_51": "小屋ダム",
    "4369_7_61": "犀川ダム", "4369_7_62": "内川ダム", "4369_7_63": "新内川ダム",
    "4369_7_64": "赤瀬ダム", "4369_7_65": "辰巳ダム", "21565_7_1": "手取川ダム",
}
COLS = ["station_id", "dam_jp", "observed_at", "water_level_masl",
        "total_inflow_m3s", "total_outflow_m3s", "storage_mcm"]


def get(url: str, attempts: int = 3) -> bytes | None:
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": ROOT + "/"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except (urllib.error.URLError, OSError, TimeoutError):
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
    return None


def num(raw, factor: float) -> float | None:
    s = str(raw).replace(",", "").strip()
    if s in MISSING:
        return None
    try:
        return round(float(s) * factor, 6)
    except ValueError:
        return None


def parse(raw: bytes) -> list[dict]:
    text = raw.decode("utf-8", errors="replace")
    i = text.find("{")
    if i < 0:
        return []                       # a soft-404 serves the site's HTML page
    try:
        payload = json.loads(text[i:])
    except json.JSONDecodeError:
        return []
    rows = []
    for sid, node in payload.items():
        if sid in ("updateTime", "observationTime") or not isinstance(node, dict):
            continue
        for point in node.get("data60") or []:
            if not isinstance(point, dict):
                continue
            tk = str(point.get("time", ""))
            parts = tk.split("-")
            if len(parts) != 5:
                continue
            y, mo, d, hh, mm = parts
            rec = {"station_id": sid, "dam_jp": STATIONS.get(sid, ""),
                   "observed_at": f"{y}-{mo}-{d}T{hh}:{mm}"}
            got = False
            for code, (var, fac) in FIELDS.items():
                cell = point.get(code)
                if not isinstance(cell, dict):
                    continue
                v = num(cell.get("val"), fac)
                if v is not None:
                    rec[var] = v
                    got = True
            if got:
                rows.append(rec)
    return rows


def main() -> int:
    day = datetime.now(JST).strftime("%Y%m%d")
    url = f"{ROOT}/dyn/dps/timeline/{day}/{day}_1_dam_60.json"
    raw = get(url)
    if raw is None:
        print(f"fetch failed: {url}")
        return 1
    RAW.mkdir(parents=True, exist_ok=True)
    (RAW / "dam_60_latest.json").write_bytes(raw)

    rows = parse(raw)
    if not rows:
        print(f"no rows parsed from {url} (soft-404 or empty day file)")
        return 1

    OUT.mkdir(parents=True, exist_ok=True)
    acc = OUT / "accumulated.csv"
    seen = set()
    if acc.exists():
        with acc.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                seen.add((r["station_id"], r["observed_at"]))
    new = [r for r in rows if (r["station_id"], r["observed_at"]) not in seen]
    write_header = not acc.exists()
    with acc.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        if write_header:
            w.writeheader()
        for r in sorted(new, key=lambda r: (r["station_id"], r["observed_at"])):
            w.writerow({c: r.get(c, "") for c in COLS})
    stamps = sorted({r["observed_at"] for r in rows})
    (OUT / "runlog_latest.json").write_text(json.dumps(
        {"run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "url": url, "stations": len({r["station_id"] for r in rows}),
         "timestamps": len(stamps), "first": stamps[0], "last": stamps[-1],
         "parsed": len(rows), "appended": len(new),
         "unmapped": sorted({r["station_id"] for r in rows if not r["dam_jp"]})},
        ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  {len({r['station_id'] for r in rows})} 站 × {len(stamps)} 时刻 "
          f"({stamps[0]} → {stamps[-1]}) = {len(rows)} 行, 累积新增 {len(new)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

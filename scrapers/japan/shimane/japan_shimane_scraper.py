#!/usr/bin/env python3
"""Shimane Prefecture dams — 島根県水防情報システム hourly JSON.

19 prefecture-managed dams. Unlike the other Japanese prefectural sources deployed so
far, this one publishes a real intra-day series rather than a single current value:

    https://www.suibou-shimane.jp/dyn/dps/json/<YYYYMMDD>/dam60.json

holds every hourly observation of that day from 00:00 onward. A day's file stays up for
a few days after the day ends: checked from a GitHub runner on 2026-10-06, the three
previous days answered complete (all 24 hours), while 20260929 and the older dates
checked on 2026-09-10 (20260901, 20260801, 20260101) 404. Every run therefore re-reads
the last DAYS_BACK days as well, so a day's evening hours no longer depend on a run
landing just before midnight.

FIELD CODES were decoded against the rendered table, not guessed. Dams whose inflow
differs from outflow disambiguate the pairs:

    7_10  貯水位 [EL.m]            -> water_level_masl
    7_50  流入量 [m3/s]            -> total_inflow_m3s
    7_70  全放流量 [m3/s]           -> total_outflow_m3s
    7_20  貯水量 [1000 m3]         -> storage_mcm  (x0.001)
    7_41  利水貯水率 洪水期 [%]      -> storage_pct_flood_season
    7_42  利水貯水率 非洪水期 [%]     -> storage_pct_nonflood_season

Not delivered: 7_30 空容量 and 7_200 空容量率 are the complement of storage rather than
an independent measurement; 7_80 and 7_120 could not be identified — for 布部ダム they
read 0.00 and 4.22 against a published 全放流量 of 4.22, so their meaning is not
established and guessing would mislabel a variable.

The two 利水貯水率 columns use different seasonal denominators and the applicable one
depends on the date, so both are carried under distinct names rather than merged.

Missing values arrive as Japanese words -- 未収集 (not collected), 欠測 (missing) --
and must never be coerced to numbers.
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
ROOT = "https://www.suibou-shimane.jp"
REFERER = f"{ROOT}/pc/dam/2110.html"
JST = timezone(timedelta(hours=9))
# Past day files each run re-reads; they stay up for at least three days. A manual run
# can look further back after an outage (workflow input days_back).
DAYS_BACK = int(os.environ.get("DAYS_BACK") or 3)

FIELDS = {
    "7_10": ("water_level_masl", 1.0),
    "7_50": ("total_inflow_m3s", 1.0),
    "7_70": ("total_outflow_m3s", 1.0),
    "7_20": ("storage_mcm", 0.001),
    "7_41": ("storage_pct_flood_season", 1.0),
    "7_42": ("storage_pct_nonflood_season", 1.0),
}
MISSING = {"未収集", "欠測", "-", "", "－"}

# station id -> dam name. The site publishes no station master, so this was derived by
# matching each id's 貯水位 at 2026-09-10 08:30 against the rendered ダム諸量一覧表 at the
# same timestamp: all 19 matched uniquely with zero ambiguity. Re-verify the same way if
# the site adds or renames a dam; an unmapped id is emitted with its raw code so a new
# dam is visible rather than silently dropped.
STATIONS = {
    "8193_7_1": "布部ダム", "8193_7_2": "山佐ダム", "8193_7_3": "三瓶ダム",
    "8193_7_4": "八戸ダム", "8193_7_5": "浜田ダム", "8193_7_7": "御部ダム",
    "8193_7_9": "銚子ダム", "8193_7_10": "美田ダム", "8193_7_11": "大長見ダム",
    "8193_7_13": "笹倉ダム", "8193_7_14": "大峠ダム", "8193_7_16": "益田川ダム",
    "8193_7_17": "第二浜田ダム", "8193_7_18": "波積ダム", "8193_7_600": "嵯峨谷ダム",
    "8193_7_601": "津田川ダム", "8193_7_602": "清瀧ダム", "8193_7_603": "三成ダム",
    "8193_7_604": "木都賀ダム",
}
COLS = ["station_id", "dam_jp", "observed_at", "water_level_masl", "total_inflow_m3s",
        "total_outflow_m3s", "storage_mcm",
        "storage_pct_flood_season", "storage_pct_nonflood_season"]


def get(url: str, attempts: int = 3) -> bytes | None:
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REFERER})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None            # day not published yet, or already expired
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
        except (urllib.error.URLError, OSError, TimeoutError):
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
    return None


def parse(raw: bytes) -> list[dict]:
    text = raw.decode("utf-8", errors="replace")
    i = text.find("{")
    if i < 0:
        return []                      # a soft-404 serves the site's HTML page
    try:
        payload = json.loads(text[i:])
    except json.JSONDecodeError:
        return []
    rows = []
    for tk, stations in payload.items():
        if tk == "update" or not isinstance(stations, dict):
            continue
        parts = tk.split("-")
        if len(parts) != 5:
            continue
        y, mo, d, hh, mm = parts
        obs = f"{y}-{mo}-{d}T{hh}:{mm}"
        for sid, fields in stations.items():
            if not isinstance(fields, dict):
                continue
            rec = {"station_id": sid, "dam_jp": STATIONS.get(sid, ""), "observed_at": obs}
            got = False
            for code, (var, fac) in FIELDS.items():
                cell = fields.get(code)
                if not isinstance(cell, dict):
                    continue
                v = str(cell.get("dt", "")).strip()
                if v in MISSING:
                    continue
                try:
                    rec[var] = round(float(v) * fac, 6)
                    got = True
                except ValueError:
                    continue           # any other non-numeric marker is treated as missing
            if got:
                rows.append(rec)
    return rows


def main() -> int:
    today = datetime.now(JST).date()
    rows: list[dict] = []
    day_files: dict[str, int] = {}
    latest_raw = None
    for back in range(DAYS_BACK, -1, -1):
        day = (today - timedelta(days=back)).strftime("%Y%m%d")
        raw = get(f"{ROOT}/dyn/dps/json/{day}/dam60.json")
        day_rows = parse(raw) if raw is not None else []
        day_files[day] = len({r["observed_at"] for r in day_rows})
        if day_rows:
            rows += day_rows
            latest_raw = raw
    if not rows:
        print(f"no day file readable from {min(day_files)} to {max(day_files)}: {day_files}")
        return 1
    RAW.mkdir(parents=True, exist_ok=True)
    (RAW / "dam60_latest.json").write_bytes(latest_raw)

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
         "day_files": day_files, "stations": len({r["station_id"] for r in rows}),
         "timestamps": len(stamps), "first": stamps[0], "last": stamps[-1],
         "parsed": len(rows), "appended": len(new)},
        ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  {len({r['station_id'] for r in rows})} 站 × {len(stamps)} 时刻 "
          f"({stamps[0]} → {stamps[-1]}) = {len(rows)} 行, 累积新增 {len(new)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

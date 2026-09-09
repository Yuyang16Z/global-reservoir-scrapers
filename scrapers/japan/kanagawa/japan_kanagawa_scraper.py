#!/usr/bin/env python3
"""Kanagawa Prefecture dams — kanagawa-dam.jp JSON API.

Five prefecture-managed reservoirs with level, storage, inflow and outflow, none of
which appear in the MLIT/OpenGov Japan delivery.

    http://kanagawa-dam.jp/api/summary.php

WHY ONLY THE LATEST POINT IS TAKEN
Each series in the payload holds 30 indexed values and a `dt` field. The `dt` values
are NOT per-series observation times: across the 57 series they decrement by exactly
one hour in listing order (2026-09-09 18:00 down to 2026-09-07 10:00), which is a
generator cursor, not data. The spacing of the 30 points cannot be established from
the API, so assigning them timestamps would fabricate a time axis.

The LAST index is verifiable as the current value: cross-checked against the
sibling endpoint /api/water-storage-level.php, which reported sagami 76, miyagase 99,
miho 84 against last-index values 75.82, 99.00, 84.30. That value is therefore taken
and dated with the payload's own `lastUpdate`. Recovering the 30-point history needs
the real time axis first; it is deliberately left unread rather than guessed.

EXCLUSIONS
  * Aggregate series (all_*, sagamisum_*, sagamiko_and_*) sum several reservoirs and
    are not single water bodies.
  * `*_ratio` is a second percentage on an undocumented basis (sagamiko_ratio 91.90
    against sagamiko_storage_level 75.82 for the same object) and is not delivered.
  * Lake-side series duplicate the dam-side volume exactly and are the same object.

`water_level_m`, not `_masl`: miho reads -8.50 where the others read 118-310, so at
least one gauge uses a local datum and the source documents none.
"""
from __future__ import annotations

import csv
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
RAW = OUT / "raw"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
API = "http://kanagawa-dam.jp/api/summary.php"
REFERER = "http://kanagawa-dam.jp/web_data/news_mizugame.html"

# dam key -> (reservoir code, Japanese name). doushi has no lake-side series.
DAMS = {
    "sagami":    ("JPN_KNG_SAGAMI",    "相模ダム"),
    "shiroyama": ("JPN_KNG_SHIROYAMA", "城山ダム"),
    "miyagase":  ("JPN_KNG_MIYAGASE",  "宮ヶ瀬ダム"),
    "miho":      ("JPN_KNG_MIHO",      "三保ダム"),
    "doushi":    ("JPN_KNG_DOUSHI",    "道志ダム"),
}
# suffix -> (variable, factor). volume is 10^3 m3 in the payload.
FIELDS = {
    "water_level": ("water_level_m", 1.0),
    "volume": ("storage_mcm", 0.001),
    "storage_level": ("storage_pct", 1.0),
    "in": ("total_inflow_m3s", 1.0),
    "out": ("total_outflow_m3s", 1.0),
}
COLS = ["reservoir_code", "dam_jp", "measurement_date", "water_level_m",
        "storage_mcm", "storage_pct", "total_inflow_m3s", "total_outflow_m3s"]


def fetch(attempts: int = 3) -> dict | None:
    for i in range(attempts):
        try:
            req = urllib.request.Request(API, headers={"User-Agent": UA, "Referer": REFERER})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError):
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
    return None


def last_value(series) -> float | None:
    """Latest point of a series. Ignores `dt`, which is a generator cursor."""
    if not isinstance(series, dict):
        return None
    idx = [int(k) for k in series if k.isdigit()]
    if not idx:
        return None
    v = series[str(max(idx))]
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parse(payload: dict) -> tuple[list[dict], str | None]:
    date = payload.get("lastUpdate")
    values = payload.get("values") or {}
    if not date or not values:
        return [], date
    rows = []
    for key, (code, jp) in DAMS.items():
        rec = {"reservoir_code": code, "dam_jp": jp, "measurement_date": date}
        got = False
        for suffix, (var, fac) in FIELDS.items():
            v = last_value(values.get(f"{key}_{suffix}"))
            if v is not None:
                rec[var] = round(v * fac, 6)
                got = True
        if got:
            rows.append(rec)
    return rows, date


def main() -> int:
    payload = fetch()
    if payload is None:
        print("fetch failed")
        return 1
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    RAW.mkdir(parents=True, exist_ok=True)
    (RAW / "summary_latest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    rows, date = parse(payload)
    if not rows:
        print(f"no rows parsed (lastUpdate={date})")
        return 1

    OUT.mkdir(parents=True, exist_ok=True)
    acc = OUT / "accumulated.csv"
    seen = set()
    if acc.exists():
        with acc.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                seen.add((r["reservoir_code"], r["measurement_date"]))
    new = [r for r in rows if (r["reservoir_code"], r["measurement_date"]) not in seen]
    write_header = not acc.exists()
    with acc.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        if write_header:
            w.writeheader()
        for r in sorted(new, key=lambda r: r["reservoir_code"]):
            w.writerow({c: r.get(c, "") for c in COLS})
    (OUT / "runlog_latest.json").write_text(json.dumps(
        {"run_at": stamp, "lastUpdate": date, "parsed": len(rows), "appended": len(new)},
        ensure_ascii=False, indent=2), encoding="utf-8")
    for r in rows:
        print(f"  {r['reservoir_code']:<20} {r['dam_jp']:<12} "
              + " ".join(f"{k.split('_')[0][:5]}={r[k]}" for k in COLS[3:] if k in r))
    print(f"\n  lastUpdate={date}  解析 {len(rows)} 行, 累积新增 {len(new)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

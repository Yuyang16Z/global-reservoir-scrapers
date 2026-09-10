#!/usr/bin/env python3
"""Japan — 川の防災情報 (river.go.jp/kawabou), the MLIT national river portal.

ONE source covering dams nationwide: MLIT, JWA, prefectural, municipal-utility and
power-company dams alike. 912 dam stations across all 47 prefectures.

WHY THIS EXISTS ALONGSIDE THE PER-PREFECTURE SCRAPERS. It was found late — on the
eighth prefectural site — by following a link from the Tokyo Waterworks page. It does
not make the per-prefecture feeds redundant: they publish 貯水量 and 貯水率 for
prefectural dams, which this portal almost always withholds for the same dams. Their
dam lists are nearly a subset of this one, though — 199 of their 209 dams match a
station here by normalised name — so their value is depth, not coverage. Where they
overlap, this portal is the wider net and they are the deeper one.

ENDPOINTS, all read from the site's own Vue bundle (js/app.*.js) rather than guessed:

    file/system/tmCrntTime.json                            the site's own clock
    file/files/tmlist/past/dam/<YYYYMMDD>/<obsFcd>.json    getSelectedTmDamPast()
    file/files/tmlist/dam/<YYYYMMDD>/<HHmm>/<obsFcd>.json  current values
    file/files/master/obs/dam/<obsFcd>.json                station metadata
    file/files/obslist/idx/pref/twn/<prefCd>.json          towns, damExistFlg
    file/files/obslist/obs/twnlist/<twnCd>.json            stations in a town

TWO ENDPOINTS ARE NEEDED, because they carry different things:

    past     169 hourly points over 7 days, but ONLY storLvl / allSink / allDisch
    current   50 hourly points over ~50h, with storCap and the percentages as well

So the long window is three variables deep and the storage series is only two days
deep. Both are fetched per station and merged on (obs_fcd, observed_at). The run is
daily: a missed day costs nothing for level/inflow/outflow, and costs storage only if
two consecutive days fail. The current endpoint's <HHmm> is read from tmCrntTime.json,
the clock the site itself builds that path from, not rounded by guesswork.

QUALITY CODES ARE NOT OPTIONAL. A missing reading is published as **0 with a code**,
never as null: 佐賀県河内ダム returns storLvl 0.0 / storLvlCcd 140 (閉局, station
closed). Importing the value without reading the code writes a 0 m water level for a
decommissioned dam. The threshold below is the app's own, used identically for stage,
rainfall, water quality and dam fields in its source:

    valid  <=>  value is not None and ccd is not None and ccd < 128
    (190 欠測 missing · 140 閉局 closed · 160 not measured — all >= 128)

That is the only filtering done here. Zeros published under a valid code are kept;
whether each one is a sentinel is a delivery-layer judgement (see README).

FIELDS as published:
    storLvl      貯水位     m (above dam datum)
    storCap      貯水量     1000 m3 -> x0.001 -> mcm   (effective storage)
    allSink      全流入量   m3/s
    allDisch     全放流量   m3/s
    storPcntIrr  利水貯水率 %

Prefectural and utility dams generally publish storLvl + allSink + allDisch only;
storCap and storPcntIrr are usually withheld and arrive coded >=128.

CROSS-CHECK: 小河内ダム read here as storCap 140,190 (1000 m3) at storPcntIrr 75.6%
against Tokyo Waterworks' own page reporting 14,141.6 万m3 at 76.3% for the same
reservoir at a nearby timestamp — independent agreement on both value and unit.

STORAGE: one gzipped CSV per JST observation day under timeseries/hourly/, written with
a fixed gzip mtime and only when the decompressed content changes. The date in each
filename is what scripts/monitor_source_freshness.py reads — it does not open .csv.gz —
so renaming the partitions would silently drop this source from monitoring.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
SERIES = OUT / "timeseries" / "hourly"
META = OUT / "metadata" / "japan_kawabou_dams.csv"
RUN_LOGS = OUT / "run_logs"
ROOT = "https://www.river.go.jp/kawabou/file/files"
SYSTEM = "https://www.river.go.jp/kawabou/file/system"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
REFERER = "https://www.river.go.jp/kawabou/pc/tm"
JST = timezone(timedelta(hours=9))
PACE = float(os.environ.get("KAWABOU_PACE", "0.7"))
CTX = ssl.create_default_context()

CCD_VALID_BELOW = 128           # the site's own threshold; >=128 means no observation
FIELDS = {
    "storLvl": ("water_level_m", 1.0),
    "storCap": ("storage_mcm", 0.001),
    "allSink": ("total_inflow_m3s", 1.0),
    "allDisch": ("total_outflow_m3s", 1.0),
    "storPcntIrr": ("storage_pct_usable", 1.0),
}
COLS = ["obs_fcd", "dam_jp", "pref", "observed_at",
        "water_level_m", "storage_mcm", "total_inflow_m3s",
        "total_outflow_m3s", "storage_pct_usable"]
META_COLS = ["obs_fcd", "dam_jp", "dam_kana", "pref", "town", "river", "operator",
             "latitude", "longitude", "address", "normal_high_stage_m", "min_stage_m"]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get(path: str, attempts: int = 3, root: str = ROOT):
    for i in range(attempts):
        try:
            req = urllib.request.Request(f"{root}/{path}",
                                         headers={"User-Agent": UA, "Referer": REFERER})
            with urllib.request.urlopen(req, timeout=60, context=CTX) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
        except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError):
            pass
        if i < attempts - 1:
            time.sleep(3 * (i + 1))
    return None


def value(point: dict, key: str, factor: float):
    """A reading is real only if its companion quality code says so."""
    v = point.get(key)
    ccd = point.get(key + "Ccd")
    if v is None or ccd is None or ccd >= CCD_VALID_BELOW:
        return None
    try:
        return round(float(v) * factor, 6)
    except (TypeError, ValueError):
        return None


def parse(payload: dict, station: dict, key: str = "pastValues") -> list[dict]:
    rows = []
    for p in payload.get(key) or []:
        stamp = str(p.get("obsTime") or "")
        if len(stamp) < 16:
            continue
        rec = {"obs_fcd": station["obsFcd"], "dam_jp": station.get("obsNm", ""),
               "pref": station.get("prefNm", ""),
               "observed_at": stamp[:10].replace("/", "-") + "T" + stamp[11:16]}
        got = False
        for field, (col, factor) in FIELDS.items():
            v = value(p, field, factor)
            if v is not None:
                rec[col] = v
                got = True
        if got:
            rows.append(rec)
    return rows


def partition_path(day: str) -> Path:
    return SERIES / f"japan_kawabou_hourly_{day}.csv.gz"


def load_partition(path: Path) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        return {(r["obs_fcd"], r["observed_at"]): r for r in csv.DictReader(fh)}


def render_partition(rows: dict[tuple[str, str], dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLS, lineterminator="\n")
    w.writeheader()
    for key in sorted(rows):
        w.writerow({c: rows[key].get(c, "") for c in COLS})
    return buf.getvalue()


def write_partition(path: Path, rows: dict[tuple[str, str], dict]) -> bool:
    """Write only when the content changed, and byte-stably when it did.

    gzip.open() stamps the current time into the gzip header, so identical rows would
    produce a new git blob on every run. mtime=0 removes that; comparing decompressed
    text rather than compressed bytes also stops a zlib version change on the runner
    from rewriting every file in the window.
    """
    text = render_partition(rows)
    if path.exists():
        try:
            with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
                if fh.read() == text:
                    return False
        except (OSError, EOFError):
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
        gz.write(text.encode("utf-8"))
    path.write_bytes(raw.getvalue())
    return True


def refresh_master(stations: list[dict]) -> None:
    """Static per-station metadata; fetched once per station, then left alone."""
    known = {}
    if META.exists():
        with META.open(encoding="utf-8-sig", newline="") as fh:
            known = {r["obs_fcd"]: r for r in csv.DictReader(fh)}
    missing = [s for s in stations if s["obsFcd"] not in known]
    if not missing:
        return
    for s in missing:
        m = get(f"master/obs/dam/{s['obsFcd']}.json")
        time.sleep(PACE)
        info = (m or {}).get("obsInfo") or {}
        if not info:
            continue                       # retried next run rather than stored empty
        known[s["obsFcd"]] = {
            "obs_fcd": s["obsFcd"], "dam_jp": info.get("obsNm") or s.get("obsNm", ""),
            "dam_kana": info.get("obsKana", ""), "pref": info.get("prefNm") or s.get("prefNm", ""),
            "town": info.get("twnNm") or s.get("twnNm", ""), "river": info.get("rvrNm", ""),
            "operator": info.get("jrsNm", ""), "latitude": info.get("lat", ""),
            "longitude": info.get("lon", ""), "address": info.get("obsAdr", ""),
            "normal_high_stage_m": info.get("nrmlHighStg", ""),
            "min_stage_m": info.get("minStg", ""),
        }
    META.parent.mkdir(parents=True, exist_ok=True)
    with META.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=META_COLS)
        w.writeheader()
        for fcd in sorted(known):
            w.writerow({c: known[fcd].get(c, "") for c in META_COLS})


def write_run_log(summary: dict) -> None:
    RUN_LOGS.mkdir(parents=True, exist_ok=True)
    stamp = summary["started_at"].replace("-", "").replace(":", "").replace("T", "_")[:15]
    (RUN_LOGS / f"{stamp}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    started_at = utc_now()
    stations = json.loads((BASE_DIR / "stations.json").read_text(encoding="utf-8"))
    stations = [s for s in stations if "合計" not in (s.get("obsNm") or "")]
    day = datetime.now(JST).strftime("%Y%m%d")

    clock = get("tmCrntTime.json", root=SYSTEM) or {}
    crnt = str(clock.get("crntObsTime") or "")
    slot = crnt[11:13] + crnt[14:16] if len(crnt) >= 16 else ""
    crnt_day = crnt[:10].replace("/", "") if len(crnt) >= 16 else day

    parsed, empty, failed = 0, [], []
    partitions: dict[str, dict] = {}
    initial: dict[str, int] = {}
    for s in stations:
        merged: dict[str, dict] = {}
        ok = False
        requests = [(f"tmlist/past/dam/{day}/{s['obsFcd']}.json", "pastValues")]
        if slot:
            requests.append((f"tmlist/dam/{crnt_day}/{slot}/{s['obsFcd']}.json", "hrValues"))
        for path, key in requests:
            payload = get(path)
            time.sleep(PACE)
            if payload is None:
                continue
            ok = True
            for r in parse(payload, s, key):
                merged.setdefault(r["observed_at"], {}).update(r)
        if not ok:
            failed.append(s["obsFcd"])
            continue
        if not merged:
            empty.append(f"{s.get('prefNm','')}/{s.get('obsNm','')}")
            continue
        parsed += len(merged)
        for stamp, r in merged.items():
            d = stamp[:10]
            part = partitions.get(d)
            if part is None:
                part = partitions[d] = load_partition(partition_path(d))
                initial[d] = len(part)
            prev = part.get((r["obs_fcd"], stamp))
            if prev:
                prev.update({k: v for k, v in r.items() if v not in ("", None)})
            else:
                part[(r["obs_fcd"], stamp)] = r

    summary = {
        "started_at": started_at, "day_file": day,
        "current_slot": f"{crnt_day}/{slot}" if slot else None,
        "stations_attempted": len(stations),
        "stations_with_data": len(stations) - len(empty) - len(failed),
        "stations_empty": len(empty), "fetch_failures": len(failed),
        "rows_parsed": parsed,
    }
    if not parsed:
        summary.update(status="failed", rows_added=0, partitions_written=[],
                       finished_at=utc_now())
        write_run_log(summary)
        print(f"no rows parsed ({len(failed)} fetch failures)")
        return 1

    written = [d for d in sorted(partitions) if write_partition(partition_path(d), partitions[d])]
    added = sum(len(partitions[d]) - initial[d] for d in partitions)
    refresh_master(stations)

    summary.update(status="partial" if failed else "success", rows_added=added,
                   partitions_written=written, empty_sample=sorted(empty)[:40],
                   finished_at=utc_now())
    write_run_log(summary)
    print(f"  {summary['stations_with_data']}/{len(stations)} 站有数据, 解析 {parsed} 行, "
          f"新增 {added} 行, 写入分区 {written}")
    if failed:
        print(f"  抓取失败 {len(failed)} 站")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

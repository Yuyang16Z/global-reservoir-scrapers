#!/usr/bin/env python3
"""Japanese prefectural dams — the `servletBousaiTableStatus` platform family.

Eight prefectures run the same vendor flood-information platform and answer the
identical servlet call, so one parser serves all of them:

    <base>/servlet/bousaiweb.servletBousaiTableStatus?...&dk=4&nw=1&...

Columns (Shift_JIS):
    管理者名 | 河川名 | 局名(dam) | 所在地 | 最新観測時刻
    | 貯水位[m] | 流入量[m3/s] | 貯水量[10^3 m3] | 貯水率[%] | 放流量[m3/s]

RETENTION: `current_snapshot`. Verified 2026-09-09 that historical queries
(`nw=0` with every tm format tried) return zero dam rows, and that the Wayback
Machine holds NO snapshot of this servlet at all (crawlers never reach it —
the table is loaded into a frame by JS). There is therefore no backfill path:
every row this scraper does not capture is lost permanently.

LICENCE: not yet established. The dam subsites carry no terms and the parent
prefectural portals did not yield a machine-readable licence on 2026-09-09.
Treated as `undeclared_review` until resolved, which under the repository's
own policy blocks publication to the PUBLIC archival repo.
"""
from __future__ import annotations

import argparse
import csv
import os
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
# OUTPUT_DIR is supplied by the workflow; the local default keeps a developer run
# out of the repository data tree.
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
RAW = OUT / "raw"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

QUERY = ("lod=0&sv=3&dk=4&mp=0&no=0&nw=1&tm=000101010000&sn=0&pg=1&vm=0&tvm=0&fn=0&cn=0"
         "&st=0&it=0&tsk=0&tsw=0&tk=0&sb=0&ga=4&gk=0&gk1=0&gk2=0&gk3=0&gk4=0&gk5=0&gn=0"
         "&gl=0&gw=0&gc=0&go=0&gm=0&omp=0&ost=0&og1=0&og2=0&og3=0&og4=0&og5=0&og6=0&og7=0"
         "&og8=0&og9=0&og10=0&og11=0&og12=0&rk=1&mty=0&vo=0&tmgo=&mnflg=0")

# prefecture -> servlet base. Discovered by probing the servlet path against every
# entry in MLIT's official national dam-info link directory, not by guessing.
SITES = {
    "Niigata":   ("新潟県",  "http://doboku-bousai.pref.niigata.jp/kasen"),
    "Yamagata":  ("山形県",  "http://www.kasen.pref.yamagata.jp/bousai"),
    "Fukui":     ("福井県",  "https://sabo.pref.fukui.lg.jp/bousai"),
    "Miyazaki":  ("宮崎県",  "http://kasen.pref.miyazaki.jp/bousai"),
    "Aomori":    ("青森県",  "https://www.kasensabo.bousai.pref.aomori.jp/bousai"),
    "Oita":      ("大分県",  "https://river.pref.oita.jp/bousai_c"),
    "Tokushima": ("徳島県",  "https://www.kasen.pref.tokushima.lg.jp/pc"),
    "Nara":      ("奈良県",  "http://www.kasen.pref.nara.jp/river_pub"),
}

# Header label -> (delivered variable, unit factor). Matched against the TABLE HEADER,
# never by column position: the eight sites share a servlet but NOT a column order.
# Oita:     ... 最新観測時刻 | 貯水位 | 流入量 | 貯水量 | 貯水率 | 放流量
# Yamagata: ... 最新観測時刻 | 流入量 | 全放流量 | 貯水位 | 貯水量 | 貯水率
# Niigata:  観測所名 | 最新観測時刻 | 平常時最高貯水位 | 貯水位 | ... | 流入量 | ...
# Positional mapping would silently write 流入量 into water_level_m for Yamagata.
VARMAP = [
    ("貯水量", "storage_mcm", 0.001),        # 10^3 m3 -> 10^6 m3
    ("貯水率", "storage_pct", 1.0),
    ("貯水位", "water_level_m", 1.0),
    ("全放流量", "total_outflow_m3s", 1.0),
    ("放流量", "total_outflow_m3s", 1.0),
    ("流入量", "total_inflow_m3s", 1.0),
]
# Static design/reference values that share the same table. They are NOT observations
# and must never enter a time series; longest-first so they win over the substrings above.
STATIC_COLS = ("平常時最高貯水位", "平常時最高水位", "洪水時最高水位", "洪水貯留準備水位",
               "サーチャージ水位", "計画高水流量", "洪水流入量", "洪水量", "常時満水位")
NUM = re.compile(r"^-?\d[\d,]*\.?\d*$")
ROWTIME = re.compile(r"(?:(\d{4})\s+)?(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})")
PAGETIME = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2})時(\d{1,2})分")


def fetch(base: str, attempts: int = 3) -> str | None:
    url = f"{base}/servlet/bousaiweb.servletBousaiTableStatus?{QUERY}&unq={int(time.time())}"
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA, "Referer": f"{base}/main.html"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return r.read().decode("shift_jis", errors="replace")
        except (urllib.error.URLError, OSError, TimeoutError):
            if i < attempts - 1:
                time.sleep(3 * (i + 1))
    return None


def cells(tr: str) -> list[str]:
    out = [re.sub(r"\s+", " ",
                  re.sub(r"<[^>]+>", "", c).replace("&nbsp;", " ")
                  .replace("&rarr;", "").replace("&uarr;", "").replace("&darr;", "")).strip()
           for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.S | re.I)]
    return [c for c in out if c]


def _classify_header(hdr: list[str]) -> dict[int, tuple[str, float]]:
    """Column index -> (variable, factor). Static/reference columns are dropped."""
    out: dict[int, tuple[str, float]] = {}
    for j, h in enumerate(hdr):
        if any(sc in h for sc in STATIC_COLS):
            continue
        for label, var, fac in VARMAP:
            if label in h:
                out.setdefault(j, (var, fac))
                break
    return out


def parse(html: str, pref_en: str, pref_jp: str) -> tuple[list[dict], dict]:
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I)
    page_time = None
    pm = PAGETIME.search(re.sub(r"<[^>]+>", " ", html))
    if pm:
        page_time = (f"{int(pm.group(1)):04d}-{int(pm.group(2)):02d}-{int(pm.group(3)):02d}"
                     f"T{int(pm.group(4)):02d}:{int(pm.group(5)):02d}")

    hdr, colmap, name_col, time_col = None, {}, None, None
    for tr in rows:
        c = cells(tr)
        joined = " ".join(c)
        if ("貯水位" in joined or "流入量" in joined) and ("局名" in joined or "観測所名" in joined):
            hdr = c
            colmap = _classify_header(c)
            name_col = next((j for j, h in enumerate(c) if "局名" in h or "観測所名" in h), None)
            time_col = next((j for j, h in enumerate(c) if "観測時刻" in h), None)
            break
    if hdr is None or not colmap:
        return [], {"rows": 0, "header_found": False, "reason": "no header/columns matched"}

    out, no_time = [], 0
    for tr in rows:
        c = cells(tr)
        if len(c) < 3 or "ダム" not in " ".join(c):
            continue
        if hdr is not None and c[:3] == hdr[:3]:
            continue
        dam = None
        if name_col is not None and name_col < len(c) and "ダム" in c[name_col]:
            dam = c[name_col]
        if dam is None:
            dam = next((x for x in c if "ダム" in x), None)
        if dam is None:
            continue
        dam = dam.split("(")[0].split("（")[0].strip()

        obs = page_time
        cand = c[time_col] if (time_col is not None and time_col < len(c)) else " ".join(c)
        rm = ROWTIME.search(cand)
        if rm:
            yr = rm.group(1) or (page_time[:4] if page_time
                                 else str(datetime.now(timezone.utc).year))
            obs = (f"{int(yr):04d}-{int(rm.group(2)):02d}-{int(rm.group(3)):02d}"
                   f"T{int(rm.group(4)):02d}:{int(rm.group(5)):02d}")
        if obs is None:
            no_time += 1
            continue

        rec = {"prefecture": pref_en, "prefecture_jp": pref_jp, "dam_jp": dam,
               "observed_at": obs, "source_header": " | ".join(hdr)}
        for j, (var, fac) in colmap.items():
            if j >= len(c):
                continue
            v = c[j].replace(",", "").replace(" ", "")
            if not NUM.match(v):
                continue
            try:
                rec[var] = round(float(v) * fac, 6)
            except ValueError:
                pass
        if any(k in rec for _, k, _ in VARMAP):
            out.append(rec)
    return out, {"rows": len(out), "header_found": True, "skipped_no_time": no_time,
                 "mapped_columns": {hdr[j]: v for j, (v, _) in colmap.items()}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated prefecture keys")
    args = ap.parse_args()
    targets = ({k: SITES[k] for k in args.only.split(",")} if args.only else SITES)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    RAW.mkdir(parents=True, exist_ok=True)
    all_rows, log = [], []
    for key, (jp, base) in targets.items():
        html = fetch(base)
        if html is None:
            log.append({"prefecture": key, "error": "fetch failed"})
            print(f"  {key:<11} FETCH FAILED", file=sys.stderr)
            continue
        # Latest-only raw, overwritten each run. At a 30-minute cadence a timestamped
        # raw file per prefecture would add ~384 files/day (~140k/year) to a public
        # repository. The accumulated CSV is the product; raw exists to diagnose a
        # parser bug against the current page shape, so one current copy suffices.
        (RAW / f"{key}_latest.html").write_text(html, encoding="utf-8")
        rows, st = parse(html, key, jp)
        all_rows += rows
        log.append({"prefecture": key, **st})
        print(f"  {key:<11} {st['rows']:>3} 坝  列映射={st.get('mapped_columns')}"
              + (f"  跳过无时刻 {st['skipped_no_time']}" if st.get("skipped_no_time") else ""))
        time.sleep(1.5)

    if all_rows:
        OUT.mkdir(parents=True, exist_ok=True)
        cols = ["prefecture", "prefecture_jp", "dam_jp", "observed_at",
                "water_level_m", "total_inflow_m3s", "storage_mcm", "storage_pct",
                "total_outflow_m3s", "source_header"]
        # append-only accumulator: this source has no history, so the archive IS the series
        acc = OUT / "accumulated.csv"
        seen = set()
        if acc.exists():
            with acc.open(encoding="utf-8-sig", newline="") as fh:
                for r in csv.DictReader(fh):
                    seen.add((r["prefecture"], r["dam_jp"], r["observed_at"]))
        new = [r for r in all_rows
               if (r["prefecture"], r["dam_jp"], r["observed_at"]) not in seen]
        write_header = not acc.exists()
        with acc.open("a", encoding="utf-8-sig", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            if write_header:
                w.writeheader()
            for r in sorted(new, key=lambda r: (r["prefecture"], r["dam_jp"])):
                w.writerow({c: r.get(c, "") for c in cols})
        print(f"\n  本轮解析 {len(all_rows)} 行")
        print(f"  累积新增 {len(new)} 行 -> accumulated.csv")
    (OUT / "runlog_latest.json").write_text(
        json.dumps({"run_at": stamp, "per_site": log, "rows": len(all_rows)},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

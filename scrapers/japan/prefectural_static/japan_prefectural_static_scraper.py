#!/usr/bin/env python3
"""Japan — prefectural/JWA dam tables served as plain HTML (non-servlet sites).

These sites do NOT share a platform, so each one carries an explicit, hand-verified
column configuration. A generic header matcher was tried first and rejected: it
mapped Fukushima's 下流水位 (TAILWATER level) onto water_level_m, JWA's 前日貯水量
(YESTERDAY's storage) onto the same column as today's, and treated Hyogo's and Mie's
静的 capacity columns as observations. Fuzzy matching is unsafe here.

Also deliberate:
  * 貯水率 is emitted as storage_pct_usable wherever the site's own denominator is
    利水容量 / 有効貯水量 rather than gross capacity (verified arithmetically:
    Hyogo 13,026/13,300 = 97.9%, Mie 3,616/3,716 = 97.3%).
  * Static capacity columns are captured separately as `capacity_*` reference values,
    never as part of the observation series.
  * Mie is NOT included: its page publishes no observation timestamp, and stamping
    rows with fetch time would fabricate an observation date.
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

BASE_DIR = Path(__file__).resolve().parent
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "outputs"))
RAW = OUT / "raw"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# Each site: url, name column, {column index: (variable, unit factor)},
# capacity column (static, kept out of the series), and how to read the timestamp.
SITES = {
    "Fukushima": {
        "jp": "福島県",
        "url": "https://kaseninf.pref.fukushima.jp/web_pub/dam/010401_60_1_0.html",
        "name_col": 1,
        # 0=No 1=局名 2=時間雨量 3=累加雨量 4=貯水位 5=下流水位 6=下流局名
        "vars": {4: ("water_level_m", 1.0)},
        # col 5 is 下流水位 — the TAILWATER, not the reservoir. Never mapped.
        "capacity": None,
        "time": ("ymdhm", r"(20\d\d)[年/\-](\d{1,2})[月/\-](\d{1,2})日?\s*(\d{1,2})[:時](\d{2})"),
        "min_cols": 6,
    },
    "JWA_ToneArakawa": {
        "jp": "水資源機構 利根川・荒川水系",
        "url": "https://www.water.go.jp/honsya/honsya/suigen/sokuhou/toneara/index.html",
        "name_col": 0,
        # 0=ダム名 1=利水容量 2=前日貯水量 3=現在貯水量 4=現在貯水率   単位 万m3 -> mcm x0.01
        "vars": {3: ("storage_mcm", 0.01), 4: ("storage_pct_usable", 1.0)},
        # col 1 is static usable capacity; col 2 is YESTERDAY's volume and belongs to a
        # different date, so neither may share today's row.
        "capacity": (1, "capacity_usable_mcm", 0.01),
        "time": ("mdh", r"(\d{1,2})月(\d{1,2})日\s*(\d{1,2})時現在"),
        "min_cols": 5,
    },
    "Hyogo": {
        "jp": "兵庫県",
        "url": "http://web.pref.hyogo.lg.jp/kc02/ea02_000000005.html",
        "name_col": 0,
        # 0=名称 1=水系 2=管理者 3=利水容量(千m3) 4=現在の貯水量(千m3) 5=貯水率(%)
        "vars": {4: ("storage_mcm", 0.001), 5: ("storage_pct_usable", 1.0)},
        "capacity": (3, "capacity_usable_mcm", 0.001),
        "time": ("ymd", r"(20\d\d)年(\d{1,2})月(\d{1,2})日現在"),
        "min_cols": 6,
    },
}

NUM = re.compile(r"^-?\d[\d,]*\.?\d*$")


def fetch(url: str, attempts: int = 3) -> str | None:
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=45) as r:
                raw = r.read()
            for enc in ("utf-8", "shift_jis", "euc_jp"):
                try:
                    t = raw.decode(enc)
                    if "ダム" in t:
                        return t
                except UnicodeDecodeError:
                    continue
            return raw.decode("utf-8", errors="replace")
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


def read_time(html: str, spec) -> str | None:
    """Return an ISO observation stamp, or None. Fetch time is NEVER substituted."""
    kind, pat = spec
    m = re.search(pat, re.sub(r"<[^>]+>", " ", html))
    if not m:
        return None
    g = [int(x) for x in m.groups()]
    now = datetime.now(timezone.utc)
    if kind == "ymdhm":
        return f"{g[0]:04d}-{g[1]:02d}-{g[2]:02d}T{g[3]:02d}:{g[4]:02d}"
    if kind == "ymd":
        return f"{g[0]:04d}-{g[1]:02d}-{g[2]:02d}"
    if kind == "mdh":
        # no year published; assume the current year, stepping back one if that would
        # place the observation in the future (December page read in January).
        year = now.year
        cand = datetime(year, g[0], g[1], g[2], tzinfo=timezone.utc)
        if cand > now:
            year -= 1
        return f"{year:04d}-{g[0]:02d}-{g[1]:02d}T{g[2]:02d}:00"
    return None


def num(s: str, factor: float) -> float | None:
    s = s.replace(",", "").replace(" ", "")
    if not NUM.match(s):
        return None
    try:
        return round(float(s) * factor, 6)
    except ValueError:
        return None


def parse(html: str, key: str, cfg: dict) -> tuple[list[dict], list[dict], dict]:
    obs_at = read_time(html, cfg["time"])
    if obs_at is None:
        return [], [], {"rows": 0, "reason": "no observation timestamp on page"}
    rows_out, caps = [], []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I):
        c = cells(tr)
        if len(c) < cfg["min_cols"] or cfg["name_col"] >= len(c):
            continue
        name = c[cfg["name_col"]]
        if "ダム" not in name:
            continue
        name = re.sub(r"[（(].*?[）)]", "", name).strip()
        rec = {"site": key, "site_jp": cfg["jp"], "dam_jp": name, "observed_at": obs_at}
        got = False
        for idx, (var, fac) in cfg["vars"].items():
            if idx < len(c):
                v = num(c[idx], fac)
                if v is not None:
                    rec[var] = v
                    got = True
        if not got:
            continue
        rows_out.append(rec)
        if cfg["capacity"]:
            ci, cvar, cfac = cfg["capacity"]
            v = num(c[ci], cfac) if ci < len(c) else None
            if v is not None:
                caps.append({"site": key, "dam_jp": name, "variable": cvar, "value": v,
                             "observed_at": obs_at})
    return rows_out, caps, {"rows": len(rows_out), "observed_at": obs_at}


def main() -> int:
    RAW.mkdir(parents=True, exist_ok=True)
    all_rows, all_caps, log = [], [], []
    for key, cfg in SITES.items():
        html = fetch(cfg["url"])
        if html is None:
            log.append({"site": key, "error": "fetch failed"})
            print(f"  {key:<18} FETCH FAILED")
            continue
        (RAW / f"{key}_latest.html").write_text(html, encoding="utf-8")
        rows, caps, st = parse(html, key, cfg)
        all_rows += rows
        all_caps += caps
        log.append({"site": key, **st})
        print(f"  {key:<18} {st['rows']:>3} 坝  observed_at={st.get('observed_at')}"
              + (f"  ({st['reason']})" if st.get("reason") else ""))
        time.sleep(1.5)

    OUT.mkdir(parents=True, exist_ok=True)
    cols = ["site", "site_jp", "dam_jp", "observed_at",
            "water_level_m", "storage_mcm", "storage_pct_usable"]
    acc = OUT / "accumulated.csv"
    seen = set()
    if acc.exists():
        with acc.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                seen.add((r["site"], r["dam_jp"], r["observed_at"]))
    new = [r for r in all_rows if (r["site"], r["dam_jp"], r["observed_at"]) not in seen]
    write_header = not acc.exists()
    with acc.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        if write_header:
            w.writeheader()
        for r in sorted(new, key=lambda r: (r["site"], r["dam_jp"])):
            w.writerow({c: r.get(c, "") for c in cols})
    if all_caps:
        with (OUT / "reference_values.csv").open("w", encoding="utf-8-sig", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["site", "dam_jp", "variable", "value", "observed_at"])
            w.writeheader()
            w.writerows(sorted(all_caps, key=lambda r: (r["site"], r["dam_jp"])))
    (OUT / "runlog_latest.json").write_text(json.dumps(
        {"run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "per_site": log, "rows": len(all_rows), "appended": len(new),
         "reference_values": len(all_caps)}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  本轮 {len(all_rows)} 行, 累积新增 {len(new)}, 静态参照值 {len(all_caps)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

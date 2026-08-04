"""Zimbabwe ZINWA national dam-levels page scraper.

Source:
- https://zinwa.co.zw/dam-levels/ (Zimbabwe National Water Authority)

Known quirks:
- A browser User-Agent is required; bare clients are rejected.
- The page updates roughly weekly on Mondays (Africa/Harare = UTC+2) and is
  frequently stale, so the observation date is ALWAYS the page's own printed
  date ("Official <d Month yyyy>", "... as at <date>", "Platform | <date>"),
  never the fetch timestamp.
- Current format ("era C", 2026-): inline JS arrays per catchment, with
  several binding styles seen in the wild:
    gwayiDams = [ {...}, ... ]
    const gwayi = [ {...}, ... ]
    const x = { ... dams: [ {...} ] }
  Each object: { name: "X", purpose: "IR", net: <FSC Mm3>,
  present: <Mm3>, pct: <%>, w: <weekly % change>, m: <monthly % change> }.
- Older formats are kept as fallbacks in case the site reverts:
  era A (2017-2022) HTML table rows (name | FSC Mm3 | current Mm3 | % full,
  some years swap/omit columns), era B (2023-2025) Elementor text blocks
  ("<Dam name> ... Dam Level - 97.3 %", numbers may contain stray spaces
  around the decimal point).
- Dam names vary across eras; ALIAS canonicalises them (e.g. 2017
  "Tokwe Mukorsi" == later "Tugwi-Mukosi").

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
- raw/<UTC stamp>_dam_levels.html            raw snapshot per run
- timeseries/zimbabwe_zinwa_timeseries.csv   long table merged by
                                             (obs_date, dam, variable)
- metadata/zimbabwe_zinwa_dam_attributes.csv catchment/purpose per dam
- run_logs/runs.jsonl                        one JSON line per run
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
META_DIR = OUTPUT_DIR / "metadata"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

PAGE_URL = "https://zinwa.co.zw/dam-levels/"
SOURCE_AGENCY = "ZINWA"
TIMEOUT = 120
RETRIES = 3

TS_COLUMNS = ["obs_date", "dam", "variable", "value"]
ATTR_COLUMNS = ["dam", "catchment", "purpose", "last_seen_obs_date"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

ALIAS = {
    "TOKWE MUKORSI": "TUGWI MUKOSI",
    "TUGWI-MUKOSI": "TUGWI MUKOSI",
    "TUGWI MUKOSI": "TUGWI MUKOSI",
    "LAKE MUTIRIKWI": "MUTIRIKWI",
    "MUTIRIKWI": "MUTIRIKWI",
    "LAKE KARIBA": "KARIBA",
    "MANYAME (DARWENDALE)": "MANYAME",
    "DARWENDALE": "MANYAME",
    "LAKE CHIVERO": "CHIVERO",
    "EXCHANGE": "EXCHANGE",
}

AGG = ("TOTAL", "CATCHMENT", "NATIONAL", "AVERAGE", "DAM NAME", "DAM LEVEL",
       "SUMMARY", "PROVINCE")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, META_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def append_run_log(payload: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (RUN_LOG_DIR / "runs.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def get_with_retries(url: str) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            return r
        except requests.RequestException as exc:
            last_exc = exc
            print(f"[retry] {url} attempt {attempt}/{RETRIES}: {exc!r}", flush=True)
            if attempt < RETRIES:
                time.sleep(10 * attempt)
    assert last_exc is not None
    raise last_exc


# ---------------------------------------------------------------------------
# Parsing (ported from the research-layer zinwa_parser.py, self-contained)
# ---------------------------------------------------------------------------

def canon(name: str) -> str:
    s = html_mod.unescape(name)
    s = re.sub(r"[^A-Za-z' \-]", " ", s).upper()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s*-\s*", "-", s)
    s = ALIAS.get(s, s)
    return s


def is_dam(name: str) -> bool:
    return bool(name) and len(name) >= 3 and not any(a in name for a in AGG)


def to_f(s: str | None) -> float | None:
    if s is None:
        return None
    s = re.sub(r"[^\d.\-]", "", str(s).replace(",", ""))
    if not s or s in {"-", "."}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _try_date(tok: str) -> str | None:
    """Parse a captured date token without external deps (day-first)."""
    tok = tok.strip().replace(",", " ")
    if re.search(r"[A-Za-z]", tok):
        tok = tok.replace("-", " ")
    tok = re.sub(r"\s+", " ", tok).strip()
    fmts = [
        "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y",
        "%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(tok, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_asat(text: str) -> str | None:
    """Find the page's own printed data date (never the capture time)."""
    pats = [
        r"Dam\s+Levels?\s+as\s+at:?\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})",
        r"Dam\s+Levels?\s+as\s+at:?\s*([0-9]{1,2}[\s-][A-Za-z]+,?[\s-][0-9]{4})",
        r"Dam\s+Levels?\s+as\s+at:?\s*([A-Za-z]+\s+[0-9]{1,2},?\s+[0-9]{4})",
        r"Dam\s+Levels?\s+of\s+([A-Za-z]+\s+[0-9]{1,2},?\s+[0-9]{4})",
        r"Dam\s+Levels?\s+of\s+([0-9]{1,2}\s+[A-Za-z]+,?\s+[0-9]{4})",
        r"as\s+at:?\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})",
        r"as\s+at:?\s*([0-9]{1,2}[\s-][A-Za-z]+,?[\s-][0-9]{4})",
        r"as\s+at:?\s*([A-Za-z]+\s+[0-9]{1,2},?\s+[0-9]{4})",
        r"Official\s+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})",
        r"Platform\s*\|\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})",
    ]
    for p in pats:
        m = re.search(p, text, re.I)
        if m:
            parsed = _try_date(m.group(1))
            if parsed:
                return parsed
    return None


def parse_era_a(text: str) -> list[tuple[str, str, float | None]]:
    """HTML table era -> [(dam, variable, value)]."""
    rows_out: list[tuple[str, str, float | None]] = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.S):
        cells = [re.sub(r"<[^>]+>|&nbsp;", " ", c).strip()
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
        if len(cells) < 3:
            continue
        name = canon(cells[0])
        if not is_dam(name):
            continue
        fsc, cur, pct = to_f(cells[1]), to_f(cells[2]), None
        if len(cells) >= 4:
            pct = to_f(cells[3])
        # some captures have only name|fsc|pct (3 numeric-ish cols)
        if pct is None and cur is not None and fsc is not None and cur <= 110 and fsc > 200:
            pct, cur = cur, None
        if fsc is not None:
            rows_out.append((name, "full_supply_capacity_mcm", fsc))
        if cur is not None:
            rows_out.append((name, "storage_mcm", cur))
        if pct is not None and 0 <= pct <= 150:
            rows_out.append((name, "storage_pct", pct))
    return rows_out


def parse_era_b(text: str) -> list[tuple[str, str, float | None]]:
    txt = re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S)
    txt = re.sub(r"<[^>]+>", "\n", txt)
    txt = html_mod.unescape(txt)
    txt = re.sub(r"[\n\s]+", " ", txt)
    # numbers may contain stray spaces around the decimal point ("96. 8 %")
    pat = re.compile(r"([A-Z][A-Za-z' \-]{2,40}?)\s+Dam Level\s*[–-]\s*"
                     r"(\d+(?:\s*\.\s*\d+)?)\s*%")
    best: dict[str, float] = {}
    for m in pat.finditer(txt):
        name = canon(m.group(1))
        pct = to_f(m.group(2).replace(" ", ""))
        if is_dam(name) and pct is not None and 0 <= pct <= 150:
            best.setdefault(name, pct)
    return [(n, "storage_pct", p) for n, p in best.items()]


def parse_era_c(text: str) -> tuple[list[tuple[str, str, float | None]],
                                    dict[str, dict[str, str]]]:
    out: list[tuple[str, str, float | None]] = []
    attrs: dict[str, dict[str, str]] = {}
    # binding 1 (2026-07+): gwayiDams = [ {...}, ... ]
    blocks = [(arr.capitalize(), body)
              for arr, body in re.findall(r"(\w+)Dams\s*=\s*\[(.*?)\]", text, re.S)]
    # binding 2 (2026 spring): const gwayi = [ {...}, ... ]
    blocks += [(arr.capitalize(), body)
               for arr, body in re.findall(
                   r"(?:const|var|let)\s+(\w+)\s*=\s*\[(.*?)\]", text, re.S)]
    # binding 3: catchment objects with a dams: [...] member
    blocks += [(arr.capitalize(), body)
               for arr, body in re.findall(
                   r"(?:const|var|let)\s+(\w+)\s*=\s*\{[^\[]{0,400}?dams\s*:\s*\[(.*?)\]",
                   text, re.S)]
    blocks = [(c, b) for c, b in blocks if "name" in b and "net" in b]
    seen_objs: set[str] = set()
    if not blocks:
        # last resort: any {name:"..", net:.., present:..} objects, no catchment
        blocks = [("", text)]
    for catchment, body in blocks:
        for obj in re.findall(r"\{([^{}]*name\s*:[^{}]*)\}", body):
            if obj in seen_objs:
                continue
            seen_objs.add(obj)
            fields = dict(re.findall(r"(\w+)\s*:\s*\"?([^,\"}]*)\"?", obj))
            name = canon(fields.get("name", ""))
            if not is_dam(name) or "net" not in fields:
                continue
            attrs[name] = {"catchment": catchment,
                           "purpose": fields.get("purpose", "")}
            for var, key in (("full_supply_capacity_mcm", "net"),
                             ("storage_mcm", "present"),
                             ("storage_pct", "pct"),
                             ("storage_pct_change_week", "w"),
                             ("storage_pct_change_month", "m")):
                v = to_f(fields.get(key))
                if v is not None:
                    if var == "storage_pct" and not (0 <= v <= 150):
                        continue
                    out.append((name, var, v))
    return out, attrs


def parse_page(text: str):
    date = parse_asat(text)
    rows, attrs = parse_era_c(text)
    era = "C" if rows else None
    if not rows:
        rows = parse_era_a(text)
        era = "A" if rows else None
    if not rows:
        rows = parse_era_b(text)
        era = "B" if rows else None
    return date, era, rows, attrs


# ---------------------------------------------------------------------------
# Idempotent merge
# ---------------------------------------------------------------------------

def merge_csv(path: Path, columns: list[str], new_rows: list[dict],
              key_fields: list[str]) -> tuple[int, int]:
    """Read existing CSV, merge new rows by key (new wins), rewrite sorted."""
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
        elif existing[key] != norm:
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


def fmt(v: float) -> str:
    return f"{v:g}"


def main() -> int:
    ensure_dirs()
    started = utc_now_iso()
    stamp = utc_stamp()
    log = {
        "started_at": started,
        "source": "zimbabwe/zinwa",
        "url": PAGE_URL,
        "output_dir": str(OUTPUT_DIR),
        "status": "started",
        "errors": [],
    }
    try:
        r = get_with_retries(PAGE_URL)
        if r.status_code != 200 or len(r.content) < 5000:
            raise RuntimeError(f"unexpected response: HTTP {r.status_code}, "
                               f"{len(r.content)} bytes")
        raw_path = RAW_DIR / f"{stamp}_dam_levels.html"
        raw_path.write_bytes(r.content)
        print(f"[SAVE] {raw_path} ({len(r.content)}B)", flush=True)
        log["raw_file"] = raw_path.name

        text = r.content.decode("utf-8", errors="replace")
        obs_date, era, rows, attrs = parse_page(text)
        if not obs_date:
            raise RuntimeError("could not find the page's printed as-at/Official date")
        if not rows:
            raise RuntimeError("no dam rows parsed from any known page era")
        log["era"] = era
        log["obs_date"] = obs_date
        log["dams"] = len({x[0] for x in rows})

        ts_rows = [{"obs_date": obs_date, "dam": name, "variable": var,
                    "value": fmt(val)}
                   for name, var, val in rows if val is not None]
        added, updated = merge_csv(TS_DIR / "zimbabwe_zinwa_timeseries.csv",
                                   TS_COLUMNS, ts_rows,
                                   ["obs_date", "dam", "variable"])
        attr_rows = [{"dam": name, "catchment": a.get("catchment", ""),
                      "purpose": a.get("purpose", ""),
                      "last_seen_obs_date": obs_date}
                     for name, a in sorted(attrs.items())]
        if attr_rows:
            merge_csv(META_DIR / "zimbabwe_zinwa_dam_attributes.csv",
                      ATTR_COLUMNS, attr_rows, ["dam"])

        log["status"] = "ok"
        log["rows_parsed"] = len(ts_rows)
        log["rows_added"] = added
        log["rows_updated"] = updated
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        return 0
    except Exception as exc:
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

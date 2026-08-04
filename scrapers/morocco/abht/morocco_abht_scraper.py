"""Morocco ABHT (Tensift basin) dam fill-rate scraper.

Source:
- https://abht.ma/  homepage "Taux de remplissage des Barrages" widget
  (Agence du Bassin Hydraulique du Tensift, Marrakech)

Why this source exists in the repo:
- The project's original Moroccan source, ABHSM (Souss-Massa), went offline:
  www.abhsm.ma stopped answering on ports 80 and 443 while its DNS record still
  resolves, and its last retrievable bulletin is 2026-06-18. ABHT is a
  different basin agency that is still publishing, so it keeps Moroccan
  reservoir observations flowing. It does NOT replace the Souss-Massa dams -
  it covers seven other dams in the Tensift basin.

Known quirks:
- The widget is a CURRENT-VALUE SNAPSHOT: it shows one date and is overwritten,
  so anything not captured is lost.
- The observation date is the widget's own printed date (DD/MM/YYYY) next to
  the "Taux de remplissage des Barrages" caption, never the fetch date.
- Each dam line reads "<name> <volume>Mm3 <percent>%" followed on the next
  line by the day's change, also in Mm3 (signed). The change is the source's
  own daily delta, not computed here.
- Mm3 is written with a superscript 3 and may arrive as "Mm³" or "Mm3".

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/abht_home_<stamp>.html
  metadata/morocco_abht_reservoirs.csv
  timeseries/morocco_abht_timeseries.csv   (merged, idempotent)
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
META_DIR = OUTPUT_DIR / "metadata"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

HOME_URL = "https://abht.ma/"
DAM_PAGE_ROOT = "https://abht.ma/Barrage"
SOURCE_AGENCY = "ABHT (Agence du Bassin Hydraulique du Tensift)"
TIMEOUT = 90
REQUEST_ATTEMPTS = 3
REQUEST_BACKOFFS = (2, 8, 20)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

# Slugs come from the homepage dam links; names are the widget's own spelling.
DAM_SLUGS = {
    "YACOUB EL MANSOUR": "yacoub-el-mansour",
    "LALLA TAKERKOUST": "lalla-takerkoust",
    "SD MOHAMED BEN SLIMANE EL JAZOULI": "sd-mohamed-ben-slimane-el-jazouli",
    "ABOU EL ABESS SEBTI": "abou-el-abess-sebti",
    "BGE MY ABDRHMANE": "bge-my-abdrhmane",
    "SIDI DRISS": "sidi-driss",
    "HASSAN 1ER": "hassan-1er",
}

TS_COLUMNS = ["measurement_date", "reservoir_id", "reservoir_name",
              "storage_mcm", "storage_pct", "storage_change_mcm", "fetched_at"]
META_COLUMNS = ["reservoir_id", "reservoir_name", "reservoir_name_en", "country",
                "admin_unit", "river", "basin", "lat", "lon", "source_agency",
                "source_url", "data_type", "last_updated"]

DAM_LINE_RE = re.compile(
    r"^(?P<name>[A-Za-zÀ-ÿ'’\- .0-9]{3,45}?)\s+"
    r"(?P<vol>-?\d+(?:[.,]\d+)?)\s*Mm[3³]\s+"
    r"(?P<pct>-?\d+(?:[.,]\d+)?)\s*%\s*$")
CHANGE_RE = re.compile(r"^(?P<chg>-?\d+(?:[.,]\d+)?)\s*Mm[3³]\s*$")
DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, META_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def is_source_unavailable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code >= 500
    return False


def get_home() -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            r = requests.get(HOME_URL, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last_exc = exc
            print(f"[WARN] ABHT fetch attempt {attempt}/{REQUEST_ATTEMPTS}: {exc}",
                  file=sys.stderr, flush=True)
            if attempt < REQUEST_ATTEMPTS:
                time.sleep(REQUEST_BACKOFFS[min(attempt - 1, len(REQUEST_BACKOFFS) - 1)])
    assert last_exc is not None
    raise last_exc


def to_num(raw: str) -> str:
    v = (raw or "").strip().replace(",", ".")
    try:
        return f"{float(v):g}"
    except ValueError:
        return ""


def page_lines(body: bytes) -> list[str]:
    t = body.decode("utf-8", "replace")
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", t, flags=re.S)
    t = html_mod.unescape(re.sub(r"<[^>]+>", "\n", t))
    t = re.sub(r"[ \t]+", " ", t)
    return [ln.strip() for ln in t.split("\n") if ln.strip()]


def parse_home(body: bytes, fetched_at: str) -> tuple[str | None, list[dict]]:
    lines = page_lines(body)
    marker = next((i for i, ln in enumerate(lines)
                   if "Taux de remplissage" in ln), None)
    obs_date = None
    if marker is not None:
        for ln in lines[marker:marker + 6]:
            m = DATE_RE.search(ln)
            if m:
                obs_date = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                break

    rows: list[dict] = []
    seen: set[str] = set()
    # dam lines sit immediately above the caption; scan the whole page so a
    # layout reshuffle does not silently drop everything
    for i, ln in enumerate(lines):
        m = DAM_LINE_RE.match(ln)
        if not m:
            continue
        name = re.sub(r"\s+", " ", m.group("name")).strip()
        key = name.upper()
        if key not in DAM_SLUGS:
            continue
        if key in seen:
            continue
        seen.add(key)
        change = ""
        if i + 1 < len(lines):
            cm = CHANGE_RE.match(lines[i + 1])
            if cm:
                change = to_num(cm.group("chg"))
        rows.append({
            "measurement_date": obs_date or "",
            "reservoir_id": f"MA_ABHT_{DAM_SLUGS[key].upper().replace('-', '_')}",
            "reservoir_name": name,
            "storage_mcm": to_num(m.group("vol")),
            "storage_pct": to_num(m.group("pct")),
            "storage_change_mcm": change,
            "fetched_at": fetched_at,
        })
    return obs_date, rows


def build_metadata(rows: list[dict], fetched_at: str) -> list[dict]:
    out = []
    for r in rows:
        slug = r["reservoir_id"].replace("MA_ABHT_", "").lower().replace("_", "-")
        out.append({
            "reservoir_id": r["reservoir_id"],
            "reservoir_name": r["reservoir_name"],
            "reservoir_name_en": r["reservoir_name"],
            "country": "Morocco",
            "admin_unit": "Marrakech-Safi",
            "river": "",
            "basin": "Tensift",
            "lat": "",
            "lon": "",
            "source_agency": SOURCE_AGENCY,
            "source_url": f"{DAM_PAGE_ROOT}/{slug}/",
            "data_type": "in_situ",
            "last_updated": fetched_at,
        })
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
            compare = [c for c in columns if c != "fetched_at"]
            if {c: existing[key][c] for c in compare} == {c: norm[c] for c in compare}:
                continue
            updated += 1
        existing[key] = norm
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for key in sorted(existing):
            w.writerow(existing[key])
    print(f"[SAVE] {path} total={len(existing)} added={added} updated={updated}",
          flush=True)
    return added, updated


def save_summary(log: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    p = RUN_LOG_DIR / f"{utc_stamp()}_summary.json"
    with p.open("w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {p}", flush=True)


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    log = {"started_at": fetched_at, "source": "morocco/abht", "url": HOME_URL,
           "output_dir": str(OUTPUT_DIR), "status": "started", "errors": []}
    try:
        r = get_home()
        raw_path = RAW_DIR / f"abht_home_{utc_stamp()}.html"
        raw_path.write_bytes(r.content)
        print(f"[SAVE] {raw_path} ({len(r.content)}B)", flush=True)

        obs_date, rows = parse_home(r.content, fetched_at)
        log["observation_date"] = obs_date
        log["dams_parsed"] = len(rows)
        if not rows:
            raise RuntimeError("no dam rows parsed from the ABHT homepage widget")
        if not obs_date:
            raise RuntimeError("no observation date found next to the widget caption")

        added, updated = merge_csv(TS_DIR / "morocco_abht_timeseries.csv",
                                   TS_COLUMNS, rows,
                                   ["measurement_date", "reservoir_id"])
        merge_csv(META_DIR / "morocco_abht_reservoirs.csv", META_COLUMNS,
                  build_metadata(rows, fetched_at), ["reservoir_id"])
        log.update({"status": "ok", "rows_added": added, "rows_updated": updated,
                    "raw_file": raw_path.name, "finished_at": utc_now_iso()})
        save_summary(log)
        print(f"[OK] {obs_date}: {len(rows)} dams, {added} added / {updated} updated",
              flush=True)
        return 0
    except Exception as exc:  # noqa: BLE001 - scheduled job: log and signal
        if is_source_unavailable(exc):
            log["status"] = "source_unavailable"
            log["errors"].append({"message": str(exc),
                                  "error_type": exc.__class__.__name__})
            log["finished_at"] = utc_now_iso()
            save_summary(log)
            print(f"[WARN] ABHT source unavailable this run: {exc}", file=sys.stderr)
            return 0
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        save_summary(log)
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

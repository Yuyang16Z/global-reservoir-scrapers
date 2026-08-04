"""Mozambique ARA-Centro, IP hydrological bulletin scraper.

Source:
- https://aracentroip.gov.mz/wp-sitemap-posts-my_bolentins-1.xml  (bulletin index)
- https://aracentroip.gov.mz/wp-content/uploads/<yyyy>/<mm>/<name>.pdf

Known quirks:
- The server runs at roughly 11 KB/s and actively drops kept-alive connections
  mid-transfer, so every request sends `Connection: close` with a long timeout
  and several retries.
- Sitemap <loc> entries carry a `#new_tab` fragment that must be stripped, and
  a large share point at the dead build host `leeva.agency/aracentroip/...`
  which has to be rewritten onto aracentroip.gov.mz (the same path resolves).
- The observation date is the bulletin's OWN printed header date
  ("16 de Janeiro de 2024"); at least one published filename carries a typo
  (BH030_16.01.20224.pdf) that the header-date rule corrects.
- Two content shapes: the national bulletin states fill percentage and outflow
  for Cahora Bassa / Chicamba / Muda in PROSE, while the per-basin annex
  ("GESTAO DAS PRINCIPAIS ALBUFEIRAS") carries a dated table with level,
  inflow, outflow and percentage. Annex rows win over prose for the same
  reservoir-date-variable because they are the more precise record.
- Seasonal ESTIAGEM / BALANCO reports are aggregates, not daily observations,
  and are skipped.
- The archive is append-only, so this scraper only downloads bulletins absent
  from its manifest; a run that finds nothing new is a success.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/<year>/<bulletin>.pdf
  manifest.csv                       downloaded bulletin index
  timeseries/mozambique_aracentro_observations.csv   (long format, merged)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import traceback
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"
MANIFEST = OUTPUT_DIR / "manifest.csv"

HOST = "https://aracentroip.gov.mz"
SITEMAP = f"{HOST}/wp-sitemap-posts-my_bolentins-1.xml"
TIMEOUT = 900
RETRIES = 5
MAX_NEW_PER_RUN = int(os.environ.get("ARACENTRO_MAX_NEW", "40"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Connection": "close",
}

DAM_CANON = {"CAHORA BASSA": "CAHORA BASSA", "CHICAMBA": "CHICAMBA",
             "CHICAMBA REAL": "CHICAMBA", "MUDA": "MUDA", "MAVUZI": "MAVUZI",
             "PEQUENOS LIBOMBOS": "PEQUENOS LIBOMBOS"}
PT_MONTHS = {"janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5,
             "junho": 6, "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10,
             "novembro": 11, "dezembro": 12}
OBS_COLUMNS = ["obs_date", "reservoir_id", "variable", "value", "source_kind",
               "bulletin_file", "fetched_at"]
MANIFEST_COLUMNS = ["bulletin_file", "url", "obs_date", "local_path", "status"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def get_with_retries(url: str, *, expect_pdf: bool = False):
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        except requests.RequestException as exc:
            print(f"[WARN] {url.rsplit('/', 1)[-1]} attempt {attempt}/{RETRIES}: "
                  f"{exc!r}", flush=True)
            time.sleep(15 * attempt)
            continue
        if r.status_code == 404:
            return None
        ok = r.status_code == 200 and (not expect_pdf or r.content.startswith(b"%PDF"))
        if ok:
            return r
        print(f"[WARN] {url.rsplit('/', 1)[-1]} attempt {attempt}/{RETRIES}: "
              f"HTTP {r.status_code}, {len(r.content)}B", flush=True)
        time.sleep(15 * attempt)
    return None


def printed_date(text: str) -> str | None:
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zc]+)\s+de\s+(\d{4})",
                  strip_accents(text), re.I)
    if not m:
        return None
    mo = PT_MONTHS.get(m.group(2).lower())
    d, y = int(m.group(1)), int(m.group(3))
    if not mo or not (1 <= d <= 31) or not (2000 <= y <= 2100):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


def fname_date(name: str) -> str | None:
    m = re.search(r"(\d{1,2})[._-](\d{1,2})[._-](\d{4})", name)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


def row_date(tok: str) -> str | None:
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", tok)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100:
        y += 2000
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


def parse_prose(text: str, date: str) -> list[tuple[str, str, str, float]]:
    t = strip_accents(text).replace("\n", " ").replace("³", "3")
    out: list[tuple[str, str, str, float]] = []
    m = re.search(r"albufeiras?\s+de\s+([^.]{5,120}?)\s+registam?\s+n[i]?veis?\s+de\s+"
                  r"enchimento\s+de\s+(.{5,120}?)"
                  r"(?:,\s*respectivamente|,\s*com\s|\.\s)", t, re.I)
    if m:
        names = [DAM_CANON.get(n.strip().upper())
                 for n in re.split(r",\s*|\s+e\s+", m.group(1))]
        pcts = re.findall(r"([0-9]+(?:[.,][0-9]+)?)\s*%", m.group(2))
        if names and len(names) == len(pcts):
            for n, p in zip(names, pcts):
                if n:
                    out.append((date, n, "storage_pct", float(p.replace(",", "."))))
    for v, n in re.findall(r"([0-9]+(?:[.,][0-9]+)?)\s*m\s?3/s\s+para\s+(?:a\s+)?"
                           r"([A-Za-z ]{3,25})", t):
        cand = re.sub(r"\s+E(\s+.*)?$", "",
                      n.strip().upper().replace("ALBUFEIRA DE ", ""))
        name = DAM_CANON.get(cand)
        if name:
            out.append((date, name, "outflow_m3s", float(v.replace(",", "."))))
    return out


def _emit(out: list, d: str, dam: str, nums: list[float]) -> None:
    """Annex column order: cota [m], afluente, efluente, enchimento [%]
    (a trailing static NPA cota may follow and is dropped)."""
    if not nums:
        return
    if 5 <= nums[0] <= 3000:
        out.append((d, dam, "water_level_m", nums[0]))
    rest = nums[1:]
    if len(rest) >= 3:
        out.append((d, dam, "inflow_m3s", rest[0]))
        out.append((d, dam, "outflow_m3s", rest[1]))
        if 0 <= rest[2] <= 150:
            out.append((d, dam, "storage_pct", rest[2]))
    elif len(rest) == 2:
        # (afluente, efluente) when clearly flow-sized or the dam has no
        # enchimento column (Mavuzi); otherwise keep only the leading value
        if rest[1] > 150 or dam == "MAVUZI":
            out.append((d, dam, "inflow_m3s", rest[0]))
            out.append((d, dam, "outflow_m3s", rest[1]))
        else:
            out.append((d, dam, "inflow_m3s", rest[0]))
    elif len(rest) == 1 and 0 <= rest[0] <= 150:
        out.append((d, dam, "storage_pct", rest[0]))


def parse_annex(text: str) -> list[tuple[str, str, str, float]]:
    out: list[tuple[str, str, str, float]] = []
    t = strip_accents(text)
    if "ALBUFEIRAS" not in t.upper():
        return out
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    blocks: list[tuple[str | None, list[tuple[str, list[float]]]]] = []
    cur_name: str | None = None
    cur_rows: list[tuple[str, list[float]]] = []

    def flush() -> None:
        nonlocal cur_name, cur_rows
        if cur_rows:
            blocks.append((cur_name, cur_rows))
        cur_name, cur_rows = None, []

    for ln in lines:
        name_hit = None
        for cand, canon in DAM_CANON.items():
            if re.search(rf"\b{re.escape(cand)}\b", strip_accents(ln).upper()):
                name_hit = canon
                break
        toks = ln.split()
        di = next((i for i, tok in enumerate(toks) if row_date(tok)), None)
        if di is None:
            if name_hit:
                if cur_rows:
                    cur_name = name_hit
                else:
                    flush()
                    cur_name = name_hit
            continue
        d = row_date(toks[di])
        # each block repeats the same recent dates, so a non-increasing date
        # marks the start of the next reservoir's block
        if cur_rows and d is not None and d <= cur_rows[-1][0]:
            flush()
        if name_hit:
            cur_name = name_hit
        nums = [float(tok.replace(",", ".")) for tok in toks[di + 1:]
                if re.match(r"^-?\d+(?:[.,]\d+)?$", tok)]
        if d and nums:
            cur_rows.append((d, nums))
    flush()
    for name, rows in blocks:
        if name:
            for d, nums in rows:
                _emit(out, d, name, nums)
    return out


def parse_bulletin(path: Path) -> tuple[str | None, list[dict]]:
    if re.search(r"ESTIAGEM|BALANCO|EPOCA", strip_accents(path.name), re.I):
        return fname_date(path.name), []
    rows: list[dict] = []
    with pdfplumber.open(path) as pdf:
        first = pdf.pages[0].extract_text() or ""
        date = printed_date(first[:400]) or fname_date(path.name)
        if not date:
            return None, []
        for d, dam, var, val in parse_prose(first, date):
            rows.append({"obs_date": d, "reservoir_id": dam, "variable": var,
                         "value": f"{val:g}", "source_kind": "prose"})
        for page in pdf.pages:
            t = page.extract_text() or ""
            if "LBUFEIRA" in t.upper():
                for d, dam, var, val in parse_annex(t):
                    rows.append({"obs_date": d, "reservoir_id": dam,
                                 "variable": var, "value": f"{val:g}",
                                 "source_kind": "table"})
    return date, rows


def load_manifest() -> dict[str, dict]:
    out: dict[str, dict] = {}
    if MANIFEST.exists():
        with MANIFEST.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                out[row["bulletin_file"]] = row
    return out


def save_manifest(manifest: dict[str, dict]) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for key in sorted(manifest):
            w.writerow(manifest[key])
    print(f"[SAVE] {MANIFEST} entries={len(manifest)}", flush=True)


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
        old = existing.get(key)
        if old is None:
            added += 1
        else:
            # the dated annex table outranks the prose statement
            if old.get("source_kind") == "table" and norm["source_kind"] == "prose":
                continue
            compare = [c for c in columns if c not in ("fetched_at", "bulletin_file")]
            if {c: old[c] for c in compare} == {c: norm[c] for c in compare}:
                continue
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


def append_run_log(log: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = RUN_LOG_DIR / f"{utc_stamp()}_summary.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {path}", flush=True)


def sitemap_pdfs() -> list[str]:
    r = get_with_retries(SITEMAP)
    if r is None:
        raise RuntimeError("bulletin sitemap unavailable after retries")
    urls = []
    for loc in re.findall(r"<loc>([^<]+)</loc>", r.text):
        u = loc.strip().split("#")[0]
        if not u.lower().rstrip("/").endswith(".pdf"):
            continue
        u = re.sub(r"^https?://[^/]*leeva\.agency/aracentroip", HOST, u)
        urls.append(u)
    return sorted(set(urls))


def main() -> int:
    ensure_dirs()
    fetched_at = utc_now_iso()
    log = {"started_at": fetched_at, "source": "mozambique/aracentro",
           "url": SITEMAP, "output_dir": str(OUTPUT_DIR), "status": "started",
           "errors": []}
    try:
        urls = sitemap_pdfs()
        manifest = load_manifest()
        todo = [u for u in urls
                if requests.utils.unquote(u.rsplit("/", 1)[1]).replace(" ", "_")
                not in manifest]
        log["sitemap_pdfs"] = len(urls)
        log["new_bulletins"] = len(todo)
        if len(todo) > MAX_NEW_PER_RUN:
            print(f"[INFO] {len(todo)} new bulletins; capping this run at "
                  f"{MAX_NEW_PER_RUN} (the rest follow next run)", flush=True)
            todo = todo[:MAX_NEW_PER_RUN]
            log["capped_at"] = MAX_NEW_PER_RUN

        all_rows: list[dict] = []
        downloaded = failed = 0
        for url in todo:
            name = requests.utils.unquote(url.rsplit("/", 1)[1]).replace(" ", "_")
            m = re.search(r"/uploads/(\d{4})/", url)
            year = m.group(1) if m else "unknown"
            dest = RAW_DIR / year / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            r = get_with_retries(url, expect_pdf=True)
            if r is None:
                failed += 1
                manifest[name] = {"bulletin_file": name, "url": url,
                                  "obs_date": "", "local_path": "",
                                  "status": "unavailable"}
                continue
            dest.write_bytes(r.content)
            downloaded += 1
            try:
                date, rows = parse_bulletin(dest)
            except Exception as exc:  # noqa: BLE001 - one bad PDF must not kill the run
                print(f"[WARN] parse failed for {name}: {exc!r}", flush=True)
                manifest[name] = {"bulletin_file": name, "url": url,
                                  "obs_date": "", "local_path": str(dest),
                                  "status": "parse_error"}
                continue
            for row in rows:
                row["bulletin_file"] = name
                row["fetched_at"] = fetched_at
            all_rows.extend(rows)
            manifest[name] = {"bulletin_file": name, "url": url,
                              "obs_date": date or "", "local_path": str(dest),
                              "status": "ok" if rows else "no_reservoir_data"}

        added = updated = 0
        if all_rows:
            added, updated = merge_csv(
                TS_DIR / "mozambique_aracentro_observations.csv",
                OBS_COLUMNS, all_rows, ["obs_date", "reservoir_id", "variable"])
        save_manifest(manifest)
        log.update({"status": "ok", "downloaded": downloaded, "failed": failed,
                    "values_parsed": len(all_rows), "rows_added": added,
                    "rows_updated": updated, "finished_at": utc_now_iso()})
        append_run_log(log)
        print(f"[OK] {downloaded} new bulletin(s), {len(all_rows)} values, "
              f"{added} added / {updated} updated", flush=True)
        return 0
    except Exception as exc:  # noqa: BLE001 - scheduled job: log and signal
        log["status"] = "error"
        log["errors"].append({"message": str(exc),
                              "traceback": traceback.format_exc()})
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

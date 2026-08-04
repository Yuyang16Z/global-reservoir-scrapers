"""Namibia NamWater weekly Dam Bulletin probe + parser.

Source:
- https://www.namwater.com.na/images/dambulletin/DamBulletin_YY-MM-DD.pdf
  (date-stamped URL per bulletin, published on Mondays when active)

Known quirks:
- Publication has been FROZEN since the 2024-07-08 bulletin
  (DamBulletin_24-07-08.pdf). This scraper probes the URLs for the last
  8 Mondays every run as a cheap resumption watch, and also fetches the
  known frozen bulletin once so the repository holds the latest issue.
- Single-page tabular bulletin, stable layout 2019-2024. Two column eras:
  13 value columns, and a 15-column era (2021+) that inserts an Irrigation
  Level/Capacity pair after Full Supply. Era detection: "Irrigation" in text.
- Values use comma decimals ("12,5"); the Present Water Stage cell may carry
  an 'e' source-estimate flag or read 'empty'/'dry'.
- Cells are assigned to columns by x-coordinate binning of pdfplumber words
  against per-page median column centers - NEVER by token order, which breaks
  when cells are blank. Stray flag/footnote words form tiny extra clusters
  that must be dropped (keep the best-populated clusters only).

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
- raw/DamBulletin_YY-MM-DD.pdf                  bulletin PDFs (date-stamped)
- timeseries/namibia_namwater_timeseries.csv    long table merged by
                                                (bulletin_date, dam, variable)
- run_logs/runs.jsonl                           one JSON line per run

Exit code: 0 when probes complete (404s are the expected dormant state);
1 when every probe fails at the transport level or a newly downloaded
bulletin cannot be parsed.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pdfplumber
import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")

RAW_DIR = OUTPUT_DIR / "raw"
TS_DIR = OUTPUT_DIR / "timeseries"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

URL_TEMPLATE = "https://www.namwater.com.na/images/dambulletin/DamBulletin_{tag}.pdf"
KNOWN_FROZEN_TAG = "24-07-08"  # last published bulletin (2024-07-08)
N_MONDAYS = 8
TIMEOUT = 120
RETRIES = 3

TS_COLUMNS = ["bulletin_date", "dam", "variable", "value", "flag"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

NUM = re.compile(r"^-?\d+(?:[.,]\d+)?$")
EMPTY_MARKERS = {"empty", "dry", "n/a", "-"}

COLS13 = ["fsl_masl", "fsc_mcm", "lal_masl", "lac_mcm", "stage_masl",
          "volume_mcm", "pct_full", "change_mcm", "volume_1wk_mcm", "pct_1wk",
          "volume_last_season_mcm", "pct_last_season", "rainfall_week_mm"]
# 2021+ bulletins insert an Irrigation Level/Capacity pair after Full Supply
COLS15 = COLS13[:2] + ["irrigation_level_masl", "irrigation_capacity_mcm"] + COLS13[2:]

VAR_MAP = {
    "stage_masl": "water_level_masl",
    "volume_mcm": "storage_mcm",
    "pct_full": "storage_pct",
    "change_mcm": "change_since_last_mcm",
    "rainfall_week_mm": "rainfall_week_mm",
    "fsl_masl": "full_supply_level_masl",
    "fsc_mcm": "full_supply_capacity_mcm",
    "lal_masl": "lowest_abstraction_level_masl",
    "lac_mcm": "lowest_abstraction_capacity_mcm",
    "irrigation_level_masl": "irrigation_level_masl",
    "irrigation_capacity_mcm": "irrigation_capacity_mcm",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dirs() -> None:
    for d in (RAW_DIR, TS_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def append_run_log(payload: dict) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (RUN_LOG_DIR / "runs.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def candidate_tags() -> list[str]:
    """The last N_MONDAYS Monday dates as YY-MM-DD tags, newest first."""
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    tags = [(monday - timedelta(weeks=k)).strftime("%y-%m-%d")
            for k in range(N_MONDAYS)]
    if KNOWN_FROZEN_TAG not in tags:
        tags.append(KNOWN_FROZEN_TAG)
    return tags


def probe_pdf(tag: str) -> tuple[str, Path | None]:
    """Return (status, saved_path). status: saved|exists|absent|error."""
    dest = RAW_DIR / f"DamBulletin_{tag}.pdf"
    if dest.exists() and dest.stat().st_size > 5000:
        return "exists", dest
    url = URL_TEMPLATE.format(tag=tag)
    last_exc: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last_exc = exc
            print(f"[retry] {url} attempt {attempt}/{RETRIES}: {exc!r}", flush=True)
            if attempt < RETRIES:
                time.sleep(10 * attempt)
            continue
        if r.status_code == 404:
            return "absent", None
        if r.status_code == 200 and r.content.startswith(b"%PDF"):
            tmp = dest.with_suffix(".part")
            tmp.write_bytes(r.content)
            tmp.rename(dest)
            print(f"[SAVE] {dest} ({len(r.content)}B)", flush=True)
            return "saved", dest
        # 200 with an HTML error body, or transient 5xx: treat as absent-ish
        print(f"[warn] {url} HTTP {r.status_code} {len(r.content)}B, not a PDF",
              flush=True)
        if attempt < RETRIES:
            time.sleep(10 * attempt)
    if last_exc is not None:
        return "error", None
    return "absent", None


# ---------------------------------------------------------------------------
# Parsing (ported from the research-layer namwater_parser.py, self-contained)
# ---------------------------------------------------------------------------

def parse_bulletin(path: Path):
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""
        words = page.extract_words()
    m = re.search(r"Date of this bulletin:\s*(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if not m:
        return None, []
    date = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    cols = COLS15 if "Irrigation" in text else COLS13

    # group words into lines by top coordinate
    lines: dict[float, list] = {}
    for w in words:
        key = round(w["top"] / 3) * 3
        lines.setdefault(key, []).append(w)
    dam_lines = []
    for top in sorted(lines):
        ws = sorted(lines[top], key=lambda w: w["x0"])
        toks = [w["text"] for w in ws]
        n_num = sum(1 for t in toks if NUM.match(t))
        first_num_i = next((i for i, t in enumerate(toks) if NUM.match(t)), None)
        if first_num_i and first_num_i >= 1 and n_num >= 4:
            name = " ".join(toks[:first_num_i])
            name = re.sub(r"^[*^#+\s]+", "", name).strip()
            name = re.sub(r"\s+DAM$", "", name.upper()).strip()
            if name and not any(k in name for k in ("BULLETIN", "TOTAL", "SYSTEM",
                                                    "NAME", "SUPPLY", "AMSL")):
                dam_lines.append((name, ws[first_num_i:]))
    if not dam_lines:
        return date, []

    # column centers: cluster x-midpoints of ALL numeric/flag cells
    xs = sorted((w["x0"] + w["x1"]) / 2
                for _, ws in dam_lines for w in ws
                if NUM.match(w["text"]) or w["text"].lower() in EMPTY_MARKERS)
    clusters: list[list[float]] = []
    for x in xs:
        if clusters and x - clusters[-1][-1] < 14:
            clusters[-1].append(x)
        else:
            clusters.append([x])
    # keep the best-populated clusters (stray flag/footnote words form tiny
    # extra clusters that would shift the ordinal column mapping)
    if len(clusters) > len(cols):
        keep = sorted(sorted(clusters, key=len, reverse=True)[:len(cols)],
                      key=lambda c: c[0])
        clusters = keep
    centers = [sum(c) / len(c) for c in clusters]
    rows = []
    for name, ws in dam_lines:
        cells: dict[int, list[str]] = {}
        for w in ws:
            mid = (w["x0"] + w["x1"]) / 2
            ci = min(range(len(centers)), key=lambda i: abs(centers[i] - mid))
            cells.setdefault(ci, []).append(w["text"])
        # map by cluster index so blank cells cannot shift later columns
        by_cluster: dict[int, tuple[str, str]] = {}
        for ci, toks in cells.items():
            joined = " ".join(toks)
            flag = "e" if any(t.lower() == "e" for t in toks) else ""
            nums = [t for t in toks if NUM.match(t)]
            if nums:
                by_cluster[ci] = (nums[0].replace(",", "."), flag)
            elif joined.strip().lower() in EMPTY_MARKERS:
                by_cluster[ci] = ("", "empty")
        if len(by_cluster) < 7:
            continue
        row = {col: by_cluster.get(i, ("", "")) for i, col in enumerate(cols)}
        rows.append((name, row))
    return date, rows


def bulletin_to_ts_rows(date: str, rows) -> list[dict]:
    out: list[dict] = []
    for name, row in rows:
        for col, (val, flag) in row.items():
            var = VAR_MAP.get(col)
            if var and val:
                out.append({"bulletin_date": date, "dam": name,
                            "variable": var, "value": val, "flag": flag})
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


def main() -> int:
    ensure_dirs()
    log = {
        "started_at": utc_now_iso(),
        "source": "namibia/namwater",
        "output_dir": str(OUTPUT_DIR),
        "status": "started",
        "probes": {},
        "errors": [],
    }
    try:
        new_pdfs: list[Path] = []
        statuses: dict[str, str] = {}
        for tag in candidate_tags():
            status, path = probe_pdf(tag)
            statuses[tag] = status
            if status == "saved" and path is not None:
                new_pdfs.append(path)
        log["probes"] = statuses

        if all(s == "error" for s in statuses.values()):
            raise RuntimeError("every bulletin probe failed at the transport level")

        parse_failures: list[str] = []
        total_rows = 0
        for pdf_path in new_pdfs:
            try:
                date, rows = parse_bulletin(pdf_path)
            except Exception as exc:
                parse_failures.append(f"{pdf_path.name}: {exc!r}")
                continue
            if not date or not rows:
                parse_failures.append(f"{pdf_path.name}: no date or rows")
                continue
            ts_rows = bulletin_to_ts_rows(date, rows)
            merge_csv(TS_DIR / "namibia_namwater_timeseries.csv", TS_COLUMNS,
                      ts_rows, ["bulletin_date", "dam", "variable"])
            total_rows += len(ts_rows)
            print(f"[ok] {pdf_path.name}: {date} {len(rows)} dams "
                  f"{len(ts_rows)} values", flush=True)

        log["new_pdfs"] = [p.name for p in new_pdfs]
        log["rows_merged"] = total_rows
        if parse_failures:
            log["errors"].extend(parse_failures)
        if new_pdfs and total_rows == 0:
            log["status"] = "error"
            log["finished_at"] = utc_now_iso()
            append_run_log(log)
            print("[ERROR] downloaded bulletin(s) but parsed no rows "
                  f"({parse_failures})", file=sys.stderr, flush=True)
            return 1

        log["status"] = "ok" if not parse_failures else "ok_with_warnings"
        log["finished_at"] = utc_now_iso()
        append_run_log(log)
        if not new_pdfs:
            print("[ok] no new bulletins (publication still dormant)", flush=True)
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

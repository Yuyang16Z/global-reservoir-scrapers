#!/usr/bin/env python3
"""Scrape Sri Lanka Mahaweli Authority "Latest Status of Reservoirs" bulletin.

Source: Water Management Secretariat (WMS), Mahaweli Authority of Sri Lanka —
daily two-page PDF at a FIXED URL that is overwritten each issue:
    https://mahaweli.gov.lk/WMS%20DATA/Menue-WMS%20-%20E.pdf
linked from https://mahaweli.gov.lk/wms.html. The WMS-DATA directory listing
is 403, so no server-side archive is reachable: this scraper accumulates a
permanent local archive (one dated copy per bulletin, dated by the bulletin's
own printed date) and a `wayback` subcommand can backfill historical snapshots
of the fixed URL from the Internet Archive when it is reachable.

Bulletin table (per reservoir row): gross capacity (mcm/Acft), per-row
observation date (DD-Mon), water level (m above msl), storage (mcm/Acft/%),
command area (ha/Acs), last-24h rainfall (mm), spilling/downstream discharge
(m3/s and ft3/s). Rows are grouped in sections (Power Stations, Mini Hydro,
Mahaweli System, Irrigation System).

Parsing is char-level: glyphs are clustered into tokens with an x-gap
threshold (the PDF renders some leading digits as overlapping text objects
that word-level extraction splits, e.g. "2"+"78.00"), lines are clustered
with a vertical tolerance (some cells sit ±1px off the row baseline), and
tokens are assigned to columns by nearest header-derived x-anchor.

Usage:
    python3 srilanka_mahaweli_scraper.py pull
    python3 srilanka_mahaweli_scraper.py wayback [--limit 5000]
    python3 srilanka_mahaweli_scraper.py parse
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import date, datetime
import os
from pathlib import Path
from urllib.request import Request, urlopen

import pdfplumber

BASE_DIR = Path(__file__).resolve().parent
# OUTPUT_DIR is supplied by the scheduled workflow; the local default keeps a
# developer run out of the repository data tree.
OUT = Path(os.environ.get("OUTPUT_DIR") or (BASE_DIR / "srilanka_outputs"))
RAW_DIR = OUT / "raw" / "pdf"
META_DIR = OUT / "metadata"
TS_DIR = OUT / "timeseries"
LOG_DIR = OUT / "run_logs"

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/126.0"
TIMEOUT = 45
PDF_URL = "https://mahaweli.gov.lk/WMS%20DATA/Menue-WMS%20-%20E.pdf"
PDF_URL_PLAIN = "https://mahaweli.gov.lk/WMS DATA/Menue-WMS - E.pdf"

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
SECTIONS = ("Power Stations", "Mini Hydro", "Mahaweli System", "Irrigation System")
SKIP_ROW_TOKENS = ("Sub Total", "Total", "Canal", "Feeder")

# column key -> approximate x anchor (derived from header each page; these are fallbacks)
FALLBACK_ANCHORS = {
    "cap_mcm": 190, "cap_acft": 245, "obs_date": 301, "level_msl": 344,
    "storage_mcm": 389, "storage_acft": 443, "storage_pct": 495,
    "area_ha": 531, "area_acs": 579, "rain_mm": 631, "q_m3s": 687, "q_ft3s": 744,
}
ASSIGN_TOL = 30.0


def fetch(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read()


def line_tokens(page, gap: float = 2.5, top_tol: float = 1.6):
    """chars -> tolerance-clustered lines -> gap-clustered tokens (text, x_center)."""
    lines: list[dict] = []
    for c in sorted(page.chars, key=lambda c: (c["top"], c["x0"])):
        for L in lines:
            if abs(c["top"] - L["top"]) <= top_tol:
                L["chars"].append(c)
                break
        else:
            lines.append({"top": c["top"], "chars": [c]})
    out = []
    for L in sorted(lines, key=lambda l: l["top"]):
        cs = sorted(L["chars"], key=lambda c: c["x0"])
        toks: list[tuple[str, float, float]] = []
        cur, cur_x0, last_x1 = "", 0.0, None
        for c in cs:
            if cur and c["x0"] - last_x1 > gap:
                toks.append((cur, cur_x0, last_x1))
                cur = ""
            if not cur:
                cur_x0 = c["x0"]
            cur += c["text"]
            last_x1 = c["x1"]
        if cur:
            toks.append((cur, cur_x0, last_x1))
        cleaned = []
        for t, x0, x1 in toks:
            text = re.sub(r"\s+", " ", t).strip()
            if text:
                cleaned.append((text, (x0 + x1) / 2))
        if cleaned:
            out.append(cleaned)
    return out


def header_anchors(token_lines) -> dict[str, float]:
    """Derive column anchor x-centers from the header labels on a page."""
    anchors = dict(FALLBACK_ANCHORS)
    mcm, acft = [], []
    for toks in token_lines[:14]:
        for text, x in toks:
            t = text.replace(" ", "")
            if t in ("/(mcm)", "(mcm)"):
                mcm.append(x)
            elif t in ("/(Acft)", "(Acft)"):
                acft.append(x)
            elif t == "(msl)":
                anchors["level_msl"] = x
            elif t == "%":
                anchors["storage_pct"] = x
            elif t in ("/(ha)", "(ha)"):
                anchors["area_ha"] = x
            elif t in ("/(Acs)", "(Acs)"):
                anchors["area_acs"] = x
            elif t in ("/(mm)", "(mm)"):
                anchors["rain_mm"] = x
            elif t == "Date":
                anchors["obs_date"] = x
            elif t.startswith("ft3/s"):
                anchors["q_ft3s"] = x
            elif t.startswith("m3/s") or t == "m/s":
                anchors["q_m3s"] = x
    if len(mcm) >= 2:
        anchors["cap_mcm"], anchors["storage_mcm"] = sorted(mcm)[0], sorted(mcm)[-1]
    if len(acft) >= 2:
        anchors["cap_acft"], anchors["storage_acft"] = sorted(acft)[0], sorted(acft)[-1]
    return anchors


def clean_number(text: str) -> str:
    t = text.replace(" ", "").replace(",", "").replace("%", "")
    if t in ("-", "--", ""):
        return ""
    try:
        return f"{float(t):g}"
    except ValueError:
        return ""


def parse_row_date(text: str, bulletin: date) -> str | None:
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})$", text.replace(" ", ""))
    if not m:
        return None
    day, mon = int(m.group(1)), MONTHS.get(m.group(2).title())
    if not mon:
        return None
    year = bulletin.year
    if mon > bulletin.month + 6:       # December rows on a January bulletin
        year -= 1
    try:
        return date(year, mon, day).isoformat()
    except ValueError:
        return None


def parse_pdf(path: Path) -> tuple[str | None, list[dict]]:
    rows: list[dict] = []
    bulletin: date | None = None
    with pdfplumber.open(str(path)) as pdf:
        pages = [line_tokens(pg) for pg in pdf.pages]
    for toks_lines in pages:
        for toks in toks_lines[:8]:
            joined = " ".join(t for t, _ in toks)
            m = re.search(r"(\d{4})/(\d{2})/(\d{2})", joined)
            if m:
                bulletin = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                break
        if bulletin:
            break
    if bulletin is None:
        return None, []

    for toks_lines in pages:
        anchors = header_anchors(toks_lines)
        section = ""
        for toks in toks_lines:
            joined = " ".join(t for t, _ in toks)
            for s in SECTIONS:
                if joined.replace(" ", "").startswith(s.replace(" ", "")):
                    section = s
            if not toks or not re.fullmatch(r"\d{1,2}", toks[0][0]):
                continue
            if any(k in joined for k in SKIP_ROW_TOKENS):
                continue
            name_parts = [t for t, x in toks[1:] if x < anchors["cap_mcm"] - 35]
            name = " ".join(name_parts).strip()
            if not name:
                continue
            cells: dict[str, str] = {}
            obs_date = None
            for text, x in toks[1 + len(name_parts):]:
                best_key, best_d = None, ASSIGN_TOL
                for key, ax in anchors.items():
                    d = abs(x - ax)
                    if d < best_d:
                        best_key, best_d = key, d
                if best_key is None:
                    continue
                if best_key == "obs_date":
                    obs_date = parse_row_date(text, bulletin)
                else:
                    cells.setdefault(best_key, clean_number(text))
            if obs_date is None:
                continue
            rows.append({
                "bulletin_date": bulletin.isoformat(),
                "measurement_date": obs_date,
                "reservoir_name": name,
                "section": section,
                "cap_mcm": cells.get("cap_mcm", ""),
                "water_level_masl": cells.get("level_msl", ""),
                "storage_mcm": cells.get("storage_mcm", ""),
                "storage_pct": cells.get("storage_pct", ""),
                "rain_24h_mm": cells.get("rain_mm", ""),
                "spill_downstream_m3s": cells.get("q_m3s", ""),
                "area_ha": cells.get("area_ha", ""),
                "source_file": path.name,
            })
    return bulletin.isoformat(), rows


def parse_all() -> dict:
    merged: dict[tuple[str, str], dict] = {}
    inventory = []
    conflicts = 0
    for path in sorted(RAW_DIR.glob("mahaweli_daily_*.pdf")):
        try:
            bulletin, rows = parse_pdf(path)
        except Exception as exc:  # noqa: BLE001
            inventory.append({"local_file": path.name, "parsed": "False", "error": repr(exc)})
            continue
        if bulletin is None:
            inventory.append({"local_file": path.name, "parsed": "False", "error": "no bulletin date"})
            continue
        for r in rows:
            key = (r["reservoir_name"], r["measurement_date"])
            if key in merged and merged[key]["water_level_masl"] != r["water_level_masl"]:
                conflicts += 1
            merged[key] = r  # newest bulletin wins (files sorted by date)
        inventory.append({"local_file": path.name, "parsed": "True", "error": "",
                          "bulletin_date": bulletin, "rows": len(rows)})

    TS_DIR.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)
    out_rows = [merged[k] for k in sorted(merged)]
    with (TS_DIR / "srilanka_wms_reservoirs.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "bulletin_date", "measurement_date", "reservoir_name", "section",
            "cap_mcm", "water_level_masl", "storage_mcm", "storage_pct",
            "rain_24h_mm", "spill_downstream_m3s", "area_ha", "source_file"])
        w.writeheader()
        w.writerows(out_rows)
    with (META_DIR / "srilanka_pdf_inventory.csv").open("w", encoding="utf-8-sig", newline="") as f:
        fields = ["local_file", "parsed", "error", "bulletin_date", "rows"]
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(inventory)
    names = sorted({r["reservoir_name"] for r in out_rows})
    dates = sorted({r["measurement_date"] for r in out_rows})
    return {"pdfs_parsed": sum(1 for i in inventory if i["parsed"] == "True"),
            "pdfs_failed": sum(1 for i in inventory if i["parsed"] == "False"),
            "station_days": len(out_rows), "reservoirs": len(names),
            "same_day_value_conflicts": conflicts,
            "date_min": dates[0] if dates else "", "date_max": dates[-1] if dates else ""}


def cmd_pull() -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    data = fetch(PDF_URL)
    if data[:4] != b"%PDF":
        return {"command": "pull", "status": "not_a_pdf"}
    tmp = RAW_DIR / "_incoming.pdf"
    tmp.write_bytes(data)
    bulletin, rows = parse_pdf(tmp)
    if bulletin is None:
        tmp.unlink()
        return {"command": "pull", "status": "unparseable"}
    dst = RAW_DIR / f"mahaweli_daily_{bulletin}.pdf"
    if dst.exists():
        tmp.unlink()
        status = "already_have_this_issue"
    else:
        tmp.replace(dst)
        status = "new_issue_saved"
    stats = parse_all()
    return {"command": "pull", "status": status, "bulletin_date": bulletin, **stats}


def cmd_wayback(limit: int) -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cdx = ("https://web.archive.org/cdx/search/cdx?url="
           "mahaweli.gov.lk/WMS%20DATA/Menue-WMS%20-%20E.pdf"
           f"&output=text&fl=timestamp,original,statuscode&filter=statuscode:200&limit={limit}")
    try:
        text = fetch(cdx).decode("utf-8", "ignore")
    except Exception as exc:  # noqa: BLE001
        return {"command": "wayback", "status": "cdx_unreachable", "error": repr(exc)}
    snaps = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            snaps.append((parts[0], parts[1]))
    downloaded, dupes, failed = 0, 0, 0
    for ts, original in sorted(snaps):
        try:
            data = fetch(f"https://web.archive.org/web/{ts}id_/{original}")
            if data[:4] != b"%PDF":
                failed += 1
                continue
            tmp = RAW_DIR / f"_wb_{ts}.pdf"
            tmp.write_bytes(data)
            bulletin, _ = parse_pdf(tmp)
            if bulletin is None:
                tmp.unlink()
                failed += 1
                continue
            dst = RAW_DIR / f"mahaweli_daily_{bulletin}.pdf"
            if dst.exists():
                tmp.unlink()
                dupes += 1
            else:
                tmp.replace(dst)
                downloaded += 1
        except Exception:  # noqa: BLE001
            failed += 1
        time.sleep(0.6)
    stats = parse_all()
    return {"command": "wayback", "snapshots_listed": len(snaps),
            "new_issues": downloaded, "duplicate_issues": dupes, "failed": failed, **stats}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("pull")
    p_way = sub.add_parser("wayback")
    p_way.add_argument("--limit", type=int, default=5000)
    sub.add_parser("parse")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if args.cmd == "pull":
        summary = cmd_pull()
    elif args.cmd == "wayback":
        summary = cmd_wayback(args.limit)
    else:
        summary = {"command": "parse", **parse_all()}
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    (LOG_DIR / f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{args.cmd}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

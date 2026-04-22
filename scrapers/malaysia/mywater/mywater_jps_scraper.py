"""
Malaysia — MyWater Portal (JPS dams) static metadata scraper.

Source: https://mywater.gov.my/portal/Modules/SumberAir/Empangan.aspx?Q=4RRbFpCg0pU%3D
Coverage: 16 JPS-operated dams across Pahang, Selangor, Johor, Kedah, Kelantan, Perak, Perlis.

Nature of source:
- STATIC metadata only — page published by MyWater/JPS, last updated 2025-05-30.
- No water level / storage / inflow / outflow timeseries on this page.
- BATU here overlaps BATU in LUAS IWRIMS (MY_LUAS_1304). Different source IDs,
  so no row clash, but downstream normalize step should mark them as the same physical dam.

This scraper is manual-trigger only (workflow_dispatch); nothing useful happens by scheduling it.

Output layout:
  <OUTPUT_DIR>/
    metadata/malaysia_mywater_jps_reservoirs.csv   # 16 rows, static dam attributes
    raw/empangan_YYYYMMDD_HHMMSS.html              # raw HTML per fetch
    run_logs/<stamp>_summary.json
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


BASE_DIR = Path(__file__).resolve().parent

_env_out = os.environ.get("OUTPUT_DIR", "").strip()
if _env_out:
    OUTPUT_DIR = Path(_env_out).expanduser().resolve()
else:
    OUTPUT_DIR = BASE_DIR / "malaysia_mywater_outputs"

METADATA_DIR = OUTPUT_DIR / "metadata"
RAW_DIR = OUTPUT_DIR / "raw"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

PAGE_URL = "https://mywater.gov.my/portal/Modules/SumberAir/Empangan.aspx?Q=4RRbFpCg0pU%3D"
TIMEOUT = 30

SOURCE_AGENCY = "JPS (via MyWater Portal)"

# Matches each data row (<tr class="rgRow"> or <tr class="rgAltRow">) and captures its 8 <td> bodies.
ROW_PATTERN = re.compile(
    r'<tr class="(?:rgRow|rgAltRow)"[^>]*>\s*'
    + r"\s*".join([r"<td[^>]*>\s*(.*?)\s*</td>"] * 8)
    + r"\s*</tr>",
    re.DOTALL,
)


METADATA_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "reservoir_name_en",
    "country",
    "admin_unit",
    "river",
    "basin",
    "lat",
    "lon",
    "capacity_total (mcm, 10^6 m^3)",
    "dead_storage (mcm, 10^6 m^3)",
    "frl (normal pool level, NPL m)",
    "dam_height (m)",
    "crest_length (m)",
    "crest_elevation (m)",
    "catchment (sq.km)",
    "source_agency",
    "source_url",
    "last_updated",
]


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_num(raw: str) -> str:
    """Strip whitespace and thousands separators. Keep as string (no unit conversion)."""
    if raw is None:
        return ""
    s = raw.strip()
    if not s:
        return ""
    # remove thousands separators like '3,460.00' -> '3460.00'
    return s.replace(",", "")


def slugify(name: str) -> str:
    s = name.strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    return s.strip("_")


def build_reservoir_id(name: str) -> str:
    return f"MY_MYWATER_JPS_{slugify(name)}"


def parse_rows(html: str) -> list[dict]:
    matches = ROW_PATTERN.findall(html)
    rows: list[dict] = []
    for m in matches:
        # Each tuple element is inner text; strip nested tags just in case.
        vals = [re.sub(r"<[^>]+>", "", v).strip() for v in m]
        if len(vals) != 8:
            continue
        name, state, height, crest_len, crest_elev, catchment, capacity, npl = vals
        if not name:
            continue
        rows.append(
            {
                "name": name,
                "state": state,
                "height": clean_num(height),
                "crest_len": clean_num(crest_len),
                "crest_elev": clean_num(crest_elev),
                "catchment": clean_num(catchment),
                "capacity": clean_num(capacity),
                "npl": clean_num(npl),
            }
        )
    return rows


def build_metadata_row(r: dict, fetched_at: str) -> dict:
    return {
        "reservoir_id": build_reservoir_id(r["name"]),
        "reservoir_name": r["name"],
        "reservoir_name_en": r["name"],
        "country": "Malaysia",
        "admin_unit": r["state"],
        "river": "",
        "basin": "",
        "lat": "",
        "lon": "",
        "capacity_total (mcm, 10^6 m^3)": r["capacity"],
        "dead_storage (mcm, 10^6 m^3)": "",
        "frl (normal pool level, NPL m)": r["npl"],
        "dam_height (m)": r["height"],
        "crest_length (m)": r["crest_len"],
        "crest_elevation (m)": r["crest_elev"],
        "catchment (sq.km)": r["catchment"],
        "source_agency": SOURCE_AGENCY,
        "source_url": PAGE_URL,
        "last_updated": fetched_at,
    }


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"[SAVE] {path}  rows={len(rows)}")


def save_raw(html: str, poll_stamp: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"empangan_{poll_stamp}.html"
    path.write_text(html, encoding="utf-8")
    print(f"[SAVE] {path}")
    return path


def main() -> int:
    print(f"[INFO] OUTPUT_DIR = {OUTPUT_DIR}")
    for d in (METADATA_DIR, RAW_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    fetched_at = now_stamp()
    poll_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    resp = requests.get(
        PAGE_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            )
        },
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    html = resp.text
    save_raw(html, poll_stamp)

    parsed = parse_rows(html)
    print(f"[INFO] parsed {len(parsed)} dam rows")
    if not parsed:
        print("[ERROR] zero rows parsed — page HTML layout likely changed. Check raw/.")
        return 2

    metadata_rows = [build_metadata_row(r, fetched_at) for r in parsed]
    write_csv(METADATA_DIR / "malaysia_mywater_jps_reservoirs.csv", METADATA_COLUMNS, metadata_rows)

    summary: dict[str, Any] = {
        "run_time": fetched_at,
        "source_url": PAGE_URL,
        "row_count": len(metadata_rows),
        "metadata_file": str(METADATA_DIR / "malaysia_mywater_jps_reservoirs.csv"),
    }
    summary_path = RUN_LOG_DIR / f"{poll_stamp}_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SAVE] {summary_path}")

    print("[DONE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Philippines PAGASA dam reservoir scraper.

Source:
    https://www.pagasa.dost.gov.ph/flood

Behavior:
    The PAGASA flood page only publishes the **latest two days** (today + yesterday,
    08:00 AM Philippine time) for 9 major Luzon dams. This scraper captures both
    every run, merging into per-date snapshot CSVs so repeated runs are idempotent
    and history accrues one day at a time.

Env vars:
    OUTPUT_DIR            Override output root (default: data/philippines/pagasa)
    SAVE_RAW_HTML         "0" to skip saving raw HTML (default: save)

This is an in-situ (telemetered gauge) source; see `data_type` in metadata.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


SOURCE_URL = "https://www.pagasa.dost.gov.ph/flood"
SOURCE_AGENCY = "PAGASA-DOST"
TIMEOUT = 30
PH_TZ = timezone(timedelta(hours=8))

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
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
    "capacity_total (MCM)",
    "dead_storage (MCM)",
    "frl (m)",
    "dam_height (m)",
    "year_built",
    "main_use",
    "source_agency",
    "source_url",
    "data_type",
    "last_updated",
    "nhwl_observed (m)",
    "rule_curve_observed (m)",
    "observation_time_source",
]

SNAPSHOT_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "observation_time",
    "Reservoir Water Level (RWL, m)",
    "Water Level Deviation (24hr, m)",
    "Normal High Water Level (NHWL, m)",
    "Deviation from NHWL (m)",
    "Rule Curve Elevation (m)",
    "Deviation from Rule Curve (m)",
    "Gate Opening (gates)",
    "Gate Opening (meters)",
    "Inflow (cms)",
    "Outflow (cms)",
]

# Static reference data from public sources (Wikipedia, NPC, NIA, MWSS fact sheets).
# Verify against operator filings before using for formal research.
DAM_REFERENCE: dict[str, dict[str, str]] = {
    "Angat": {
        "reservoir_id": "PH_PAGASA_angat",
        "reservoir_name": "Angat Dam",
        "admin_unit": "Bulacan",
        "river": "Angat River",
        "basin": "Angat",
        "lat": "14.9153",
        "lon": "121.1669",
        "capacity_total (MCM)": "850",
        "dam_height (m)": "131",
        "year_built": "1967",
        "main_use": "water_supply, hydroelectricity, irrigation",
    },
    "Ipo": {
        "reservoir_id": "PH_PAGASA_ipo",
        "reservoir_name": "Ipo Dam",
        "admin_unit": "Bulacan",
        "river": "Angat River",
        "basin": "Angat",
        "lat": "14.9056",
        "lon": "121.1422",
        "capacity_total (MCM)": "7.5",
        "dam_height (m)": "50",
        "year_built": "1984",
        "main_use": "water_supply",
    },
    "La Mesa": {
        "reservoir_id": "PH_PAGASA_la_mesa",
        "reservoir_name": "La Mesa Dam",
        "admin_unit": "Quezon City",
        "river": "Tullahan River",
        "basin": "Tullahan",
        "lat": "14.7566",
        "lon": "121.0864",
        "capacity_total (MCM)": "50.5",
        "dam_height (m)": "47",
        "year_built": "1929",
        "main_use": "water_supply",
    },
    "Ambuklao": {
        "reservoir_id": "PH_PAGASA_ambuklao",
        "reservoir_name": "Ambuklao Dam",
        "admin_unit": "Benguet",
        "river": "Agno River",
        "basin": "Agno",
        "lat": "16.4906",
        "lon": "120.7978",
        "capacity_total (MCM)": "327",
        "dam_height (m)": "129",
        "year_built": "1956",
        "main_use": "hydroelectricity",
    },
    "Binga": {
        "reservoir_id": "PH_PAGASA_binga",
        "reservoir_name": "Binga Dam",
        "admin_unit": "Benguet",
        "river": "Agno River",
        "basin": "Agno",
        "lat": "16.4372",
        "lon": "120.7567",
        "capacity_total (MCM)": "87.5",
        "dam_height (m)": "107",
        "year_built": "1960",
        "main_use": "hydroelectricity",
    },
    "San Roque": {
        "reservoir_id": "PH_PAGASA_san_roque",
        "reservoir_name": "San Roque Dam",
        "admin_unit": "Pangasinan",
        "river": "Agno River",
        "basin": "Agno",
        "lat": "16.1428",
        "lon": "120.6939",
        "capacity_total (MCM)": "1060",
        "dam_height (m)": "200",
        "year_built": "2003",
        "main_use": "multipurpose",
    },
    "Pantabangan": {
        "reservoir_id": "PH_PAGASA_pantabangan",
        "reservoir_name": "Pantabangan Dam",
        "admin_unit": "Nueva Ecija",
        "river": "Pampanga River",
        "basin": "Pampanga",
        "lat": "15.8000",
        "lon": "121.1167",
        "capacity_total (MCM)": "3000",
        "dam_height (m)": "107",
        "year_built": "1977",
        "main_use": "multipurpose",
    },
    "Magat Dam": {
        "reservoir_id": "PH_PAGASA_magat",
        "reservoir_name": "Magat Dam",
        "admin_unit": "Isabela / Ifugao",
        "river": "Magat River",
        "basin": "Cagayan",
        "lat": "16.8286",
        "lon": "121.4519",
        "capacity_total (MCM)": "1080",
        "dam_height (m)": "114",
        "year_built": "1982",
        "main_use": "multipurpose",
    },
    "Caliraya": {
        "reservoir_id": "PH_PAGASA_caliraya",
        "reservoir_name": "Caliraya Dam",
        "admin_unit": "Laguna",
        "river": "Caliraya River",
        "basin": "Caliraya-Lumot",
        "lat": "14.2667",
        "lon": "121.5500",
        "capacity_total (MCM)": "36",
        "dam_height (m)": "38",
        "year_built": "1942",
        "main_use": "hydroelectricity",
    },
}


@dataclass
class DailyObservation:
    dam_name: str
    date: str
    observation_time: str
    rwl: str
    wl_deviation: str
    nhwl: str
    dev_nhwl: str
    rule_curve: str
    dev_rule_curve: str
    gates: str
    meters: str
    inflow: str
    outflow: str


def now_stamp() -> str:
    return datetime.now(PH_TZ).strftime("%Y-%m-%d %H:%M:%S")


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def clean_cell(value: str) -> str:
    value = clean_text(value)
    return "" if value in {"", "-", "−", "—", "–"} else value


def parse_number_text(value: str) -> str:
    value = clean_cell(value)
    if not value:
        return ""
    match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", value)
    if not match:
        return value
    return match.group(0).replace(",", "").lstrip("+")


def fetch_html(url: str, session: requests.Session) -> str:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.text


def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def resolve_date(abbr: str, today: date) -> str:
    """Convert month-day abbreviation like 'Apr-23' into ISO date anchored to today."""
    abbr = clean_text(abbr)
    if not abbr:
        return ""
    try:
        parsed = datetime.strptime(f"{abbr}-{today.year}", "%b-%d-%Y").date()
    except ValueError:
        return ""
    if parsed > today:
        parsed = parsed.replace(year=today.year - 1)
    return parsed.isoformat()


def extract_page_timestamp(soup: BeautifulSoup) -> tuple[str, date]:
    """PAGASA shows a header like 'April 23,2026 08:00:00 am' at the top of the page."""
    header = soup.find("h5", class_="pull-right")
    raw = clean_text(header.get_text(" ", strip=True)) if header else ""
    if not raw:
        return "", datetime.now(PH_TZ).date()
    normalized = raw.replace(",", ", ")
    for pattern in ("%B %d,  %Y %I:%M:%S %p", "%B %d, %Y %I:%M:%S %p"):
        try:
            return raw, datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
    try:
        alt = raw.replace(",", ",", 1)
        return raw, datetime.strptime(alt, "%B %d,%Y %I:%M:%S %p").date()
    except ValueError:
        return raw, datetime.now(PH_TZ).date()


def find_dam_table(soup: BeautifulSoup) -> Any:
    expected = [
        "Dam Name",
        "Observation Time & Date",
        "Reservoir Water Level (RWL) (m)",
    ]
    for table in soup.find_all("table"):
        headers = [clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]
        if all(label in headers for label in expected):
            return table
    raise RuntimeError("Could not find PAGASA dam status table.")


def parse_dam_table(table: Any, page_date: date) -> list[DailyObservation]:
    observations: list[DailyObservation] = []

    rows = [
        [clean_cell(cell.get_text(" ", strip=True)) for cell in tr.find_all(["td", "th"])]
        for tr in table.find_all("tr")
    ]

    current_dam: str | None = None
    current_nhwl: str = ""
    pending_today: dict[str, str] | None = None
    pending_yesterday: dict[str, str] | None = None

    def commit(pending: dict[str, str] | None, date_str: str) -> None:
        if pending and date_str:
            observations.append(
                DailyObservation(
                    dam_name=pending["dam_name"],
                    date=date_str,
                    observation_time=pending["observation_time"],
                    rwl=pending["rwl"],
                    wl_deviation=pending["wl_deviation"],
                    nhwl=pending["nhwl"],
                    dev_nhwl=pending["dev_nhwl"],
                    rule_curve=pending["rule_curve"],
                    dev_rule_curve=pending["dev_rule_curve"],
                    gates=pending["gates"],
                    meters=pending["meters"],
                    inflow=pending["inflow"],
                    outflow=pending["outflow"],
                )
            )

    for cells in rows:
        if len(cells) == 13 and cells[0] and cells[0] not in {"Dam Name"}:
            current_dam = cells[0]
            current_nhwl = parse_number_text(cells[5])
            pending_today = {
                "dam_name": current_dam,
                "observation_time": cells[1],
                "rwl": parse_number_text(cells[2]),
                "wl_deviation": parse_number_text(cells[4]),
                "nhwl": current_nhwl,
                "dev_nhwl": parse_number_text(cells[6]),
                "rule_curve": parse_number_text(cells[7]),
                "dev_rule_curve": parse_number_text(cells[8]),
                "gates": parse_number_text(cells[9]),
                "meters": parse_number_text(cells[10]),
                "inflow": parse_number_text(cells[11]),
                "outflow": parse_number_text(cells[12]),
            }
            pending_yesterday = None
        elif len(cells) == 9 and current_dam:
            pending_yesterday = {
                "dam_name": current_dam,
                "observation_time": cells[0],
                "rwl": parse_number_text(cells[1]),
                "wl_deviation": "",
                "nhwl": current_nhwl,
                "dev_nhwl": parse_number_text(cells[2]),
                "rule_curve": parse_number_text(cells[3]),
                "dev_rule_curve": parse_number_text(cells[4]),
                "gates": parse_number_text(cells[5]),
                "meters": parse_number_text(cells[6]),
                "inflow": parse_number_text(cells[7]),
                "outflow": parse_number_text(cells[8]),
            }
        elif len(cells) == 1 and cells[0]:
            date_str = resolve_date(cells[0], page_date)
            if pending_today is not None:
                commit(pending_today, date_str)
                pending_today = None
            elif pending_yesterday is not None:
                commit(pending_yesterday, date_str)
                pending_yesterday = None

    return observations


def build_metadata_row(
    dam_name: str,
    observation: DailyObservation | None,
    fetched_at: str,
) -> dict[str, str]:
    ref = DAM_REFERENCE.get(dam_name, {})
    base: dict[str, str] = {col: "" for col in METADATA_COLUMNS}
    base.update(
        {
            "reservoir_id": ref.get("reservoir_id", f"PH_PAGASA_{dam_name.lower().replace(' ', '_')}"),
            "reservoir_name": ref.get("reservoir_name", dam_name),
            "reservoir_name_en": ref.get("reservoir_name", dam_name),
            "country": "Philippines",
            "admin_unit": ref.get("admin_unit", ""),
            "river": ref.get("river", ""),
            "basin": ref.get("basin", ""),
            "lat": ref.get("lat", ""),
            "lon": ref.get("lon", ""),
            "capacity_total (MCM)": ref.get("capacity_total (MCM)", ""),
            "dead_storage (MCM)": "",
            "frl (m)": "",
            "dam_height (m)": ref.get("dam_height (m)", ""),
            "year_built": ref.get("year_built", ""),
            "main_use": ref.get("main_use", ""),
            "source_agency": SOURCE_AGENCY,
            "source_url": SOURCE_URL,
            "data_type": "in_situ",
            "last_updated": fetched_at,
            "nhwl_observed (m)": observation.nhwl if observation else "",
            "rule_curve_observed (m)": observation.rule_curve if observation else "",
            "observation_time_source": observation.observation_time if observation else "",
        }
    )
    return base


def observation_to_snapshot_row(obs: DailyObservation) -> dict[str, str]:
    ref = DAM_REFERENCE.get(obs.dam_name, {})
    reservoir_id = ref.get("reservoir_id", f"PH_PAGASA_{obs.dam_name.lower().replace(' ', '_')}")
    reservoir_name = ref.get("reservoir_name", obs.dam_name)
    return {
        "reservoir_id": reservoir_id,
        "reservoir_name": reservoir_name,
        "date": obs.date,
        "observation_time": obs.observation_time,
        "Reservoir Water Level (RWL, m)": obs.rwl,
        "Water Level Deviation (24hr, m)": obs.wl_deviation,
        "Normal High Water Level (NHWL, m)": obs.nhwl,
        "Deviation from NHWL (m)": obs.dev_nhwl,
        "Rule Curve Elevation (m)": obs.rule_curve,
        "Deviation from Rule Curve (m)": obs.dev_rule_curve,
        "Gate Opening (gates)": obs.gates,
        "Gate Opening (meters)": obs.meters,
        "Inflow (cms)": obs.inflow,
        "Outflow (cms)": obs.outflow,
    }


def merge_snapshot(path: Path, new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not path.exists():
        return sorted(new_rows, key=lambda r: r["reservoir_id"])
    existing: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            existing[row["reservoir_id"]] = row
    for row in new_rows:
        existing[row["reservoir_id"]] = row
    return sorted(existing.values(), key=lambda r: r["reservoir_id"])


def resolve_output_dir() -> Path:
    default = Path(__file__).resolve().parents[3] / "data" / "philippines" / "pagasa"
    return Path(os.environ.get("OUTPUT_DIR", str(default))).resolve()


def main() -> int:
    output_dir = resolve_output_dir()
    metadata_dir = output_dir / "metadata"
    daily_dir = output_dir / "timeseries" / "daily"
    raw_dir = output_dir / "raw" / "html"
    run_log_dir = output_dir / "run_logs"

    save_raw = os.environ.get("SAVE_RAW_HTML", "1") != "0"

    for directory in (metadata_dir, daily_dir, run_log_dir):
        directory.mkdir(parents=True, exist_ok=True)
    if save_raw:
        raw_dir.mkdir(parents=True, exist_ok=True)

    fetched_at = now_stamp()
    run_stamp = datetime.now(PH_TZ).strftime("%Y%m%d_%H%M%S")
    summary_path = run_log_dir / f"{run_stamp}_summary.json"

    summary: dict[str, Any] = {
        "run_time": fetched_at,
        "source_url": SOURCE_URL,
        "status": "started",
        "output_dir": str(output_dir),
        "metadata_file": str(metadata_dir / "philippines_reservoirs.csv"),
        "snapshot_files": [],
        "errors": [],
    }

    try:
        print(f"[INFO] OUTPUT_DIR = {output_dir}")
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        html = fetch_html(SOURCE_URL, session)
        if save_raw:
            save_text(raw_dir / f"flood_{run_stamp}.html", html)

        soup = BeautifulSoup(html, "html.parser")
        page_header, page_date = extract_page_timestamp(soup)

        table = find_dam_table(soup)
        observations = parse_dam_table(table, page_date)

        by_dam_today: dict[str, DailyObservation] = {}
        snapshots_by_date: dict[str, list[dict[str, str]]] = {}
        for obs in observations:
            snapshots_by_date.setdefault(obs.date, []).append(observation_to_snapshot_row(obs))
            if page_date.isoformat() == obs.date:
                by_dam_today[obs.dam_name] = obs

        metadata_rows = [
            build_metadata_row(dam_name, by_dam_today.get(dam_name), fetched_at)
            for dam_name in DAM_REFERENCE
        ]
        for dam_name in {o.dam_name for o in observations} - set(DAM_REFERENCE):
            metadata_rows.append(build_metadata_row(dam_name, by_dam_today.get(dam_name), fetched_at))

        metadata_rows.sort(key=lambda row: row["reservoir_id"])
        write_csv(metadata_dir / "philippines_reservoirs.csv", METADATA_COLUMNS, metadata_rows)

        snapshot_files: list[str] = []
        for snapshot_date, rows in sorted(snapshots_by_date.items()):
            path = daily_dir / f"philippines_timeseries_{snapshot_date}.csv"
            merged = merge_snapshot(path, rows)
            write_csv(path, SNAPSHOT_COLUMNS, merged)
            snapshot_files.append(str(path))

        summary.update(
            {
                "status": "success",
                "page_header": page_header,
                "page_date": page_date.isoformat(),
                "dam_count": len(metadata_rows),
                "observation_count": len(observations),
                "snapshot_files": snapshot_files,
            }
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[DONE] dams={len(metadata_rows)} observations={len(observations)} "
            f"dates={len(snapshots_by_date)} page_date={page_date.isoformat()}"
        )
        return 0
    except Exception as exc:
        summary.update(
            {
                "status": "failed",
                "fatal_error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(traceback.format_exc(), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

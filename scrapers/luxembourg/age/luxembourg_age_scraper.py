#!/usr/bin/env python3
"""Accumulate complete daily reservoir levels for Luxembourg AGE station 40."""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from statistics import fmean
from typing import Any
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", BASE_DIR / "luxembourg_age_outputs")).resolve()
METADATA_DIR = OUTPUT_DIR / "metadata"
DAILY_DIR = OUTPUT_DIR / "timeseries" / "daily"
RAW_DIR = OUTPUT_DIR / "raw" / "daily"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

GRAPH_API_URL = "https://inondations.lu/api/station/graph-data/40"
STATION_PAGE_URL = "https://inondations.lu/basins/sauer?lang=en&show-details=&station=40"
DATASET_URL = "https://data.public.lu/en/datasets/niveau-deau/"
STATION_SHEET_URL = (
    "http://geoportail.eau.etat.lu/pdf/hydrometrie/FichesStations/40-Esch-Sure.pdf"
)

RESERVOIR_ID = "LUX_AGE_40"
RESERVOIR_NAME = "Lac de la Haute-Sure"
LOCAL_TZ = ZoneInfo("Europe/Luxembourg")
TIMEOUT_SECONDS = 60

TIMESERIES_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "water_level_masl (m NN)",
    "source_observation_count",
    "expected_observation_count",
    "first_observation_local",
    "last_observation_local",
]

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
    "capacity_total (mcm)",
    "dead_storage (mcm)",
    "frl (m)",
    "dam_height (m)",
    "year_built",
    "main_use",
    "source_agency",
    "source_url",
    "data_type",
    "last_updated",
    "station_id",
    "station_name",
    "download_url",
    "license",
    "reuse_status",
    "attribution_text",
    "license_checked_at",
    "license_evidence_url",
    "license_notes",
    "scope_evidence_url",
]

METADATA_ROW = {
    "reservoir_id": RESERVOIR_ID,
    "reservoir_name": RESERVOIR_NAME,
    "reservoir_name_en": "Upper Sure Lake",
    "country": "Luxembourg",
    "admin_unit": "Esch-sur-Sure",
    "river": "Sure",
    "basin": "Sure",
    "lat": "49.9117673453",
    "lon": "5.9227900927",
    "capacity_total (mcm)": "60",
    "dead_storage (mcm)": "",
    "frl (m)": "",
    "dam_height (m)": "",
    "year_built": "1961",
    "main_use": "water_supply;hydropower;flood_control;flow_regulation",
    "source_agency": "Administration de la gestion de l'eau (AGE)",
    "source_url": STATION_PAGE_URL,
    "data_type": "in_situ",
    "last_updated": "2026-07-21 00:00:00+02:00",
    "station_id": "40",
    "station_name": "Barrage Esch-Sauer",
    "download_url": GRAPH_API_URL,
    "license": "Creative Commons Zero (CC0 1.0 Universal)",
    "reuse_status": "open_no_attribution",
    "attribution_text": (
        "Administration de la gestion de l'eau (Luxembourg), Niveau d'eau dataset. "
        "Downloaded and standardized by the Global Reservoir Dataset project."
    ),
    "license_checked_at": "2026-07-19",
    "license_evidence_url": DATASET_URL,
    "license_notes": (
        "The official data.public.lu water-level dataset assigns CC0 to the station files "
        "and listed measured-water-level APIs. The graph endpoint is a rolling recent window."
    ),
    "scope_evidence_url": STATION_SHEET_URL,
}

QUALITY_COLUMNS = [
    "reservoir_id",
    "variable",
    "start_date",
    "end_date",
    "quality_state",
    "quality_description",
]

QUALITY_ROW = {
    "reservoir_id": RESERVOIR_ID,
    "variable": "water_level_masl (m NN)",
    "start_date": "2026-07-14",
    "end_date": "",
    "quality_state": "official_complete_daily_mean",
    "quality_description": (
        "AGE in-situ dam-level points with simulated=false; project arithmetic daily mean "
        "only when every regular 15-minute observation in the Luxembourg local day is present; "
        "no interpolation or gap filling. Expected sample count is 92, 96, or 100 on DST days."
    ),
}


def ensure_dirs() -> None:
    for directory in (METADATA_DIR, DAILY_DIR, RAW_DIR, RUN_LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def fetch_json(url: str, attempts: int = 4) -> tuple[bytes, dict[str, Any]]:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": (
                        "global-reservoir-scrapers/1.0 "
                        "(https://github.com/Yuyang16Z/global-reservoir-scrapers)"
                    ),
                },
            )
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                payload = response.read()
            return payload, json.loads(payload)
        except Exception as exc:
            error = exc
            if attempt + 1 < attempts:
                time.sleep(2**attempt)
    raise RuntimeError(f"Failed after {attempts} attempts: {url}: {error}")


def expected_quarter_hours(local_date: date) -> int:
    start = datetime.combine(local_date, dt_time.min, tzinfo=LOCAL_TZ).astimezone(timezone.utc)
    end = datetime.combine(
        local_date + timedelta(days=1), dt_time.min, tzinfo=LOCAL_TZ
    ).astimezone(timezone.utc)
    return int((end - start).total_seconds() // (15 * 60))


def build_complete_daily_rows(levels: list[dict[str, Any]]) -> tuple[list[dict[str, str]], Counter]:
    counters: Counter = Counter()
    grouped: dict[date, dict[datetime, float]] = defaultdict(dict)

    for point in levels:
        counters["source_points"] += 1
        if point.get("simulated") is not False:
            counters["excluded_simulated_or_unknown"] += 1
            continue
        try:
            timestamp = float(point["time"]) / 1000
            value = float(point["level"])
        except (KeyError, TypeError, ValueError):
            counters["excluded_malformed"] += 1
            continue
        local_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(LOCAL_TZ)
        grouped[local_dt.date()][local_dt.astimezone(timezone.utc)] = value

    rows: list[dict[str, str]] = []
    for local_date, values_by_utc in sorted(grouped.items()):
        ordered = sorted(values_by_utc.items())
        expected_count = expected_quarter_hours(local_date)
        local_times = [item[0].astimezone(LOCAL_TZ) for item in ordered]
        intervals = [
            int((ordered[index][0] - ordered[index - 1][0]).total_seconds())
            for index in range(1, len(ordered))
        ]
        complete = (
            len(ordered) == expected_count
            and local_times[0].strftime("%H:%M") == "00:00"
            and local_times[-1].strftime("%H:%M") == "23:45"
            and all(interval == 15 * 60 for interval in intervals)
        )
        if not complete:
            counters["excluded_incomplete_local_day"] += 1
            continue

        rows.append(
            {
                "reservoir_id": RESERVOIR_ID,
                "reservoir_name": RESERVOIR_NAME,
                "date": local_date.isoformat(),
                "water_level_masl (m NN)": f"{fmean(values_by_utc.values()):.5f}",
                "source_observation_count": str(len(ordered)),
                "expected_observation_count": str(expected_count),
                "first_observation_local": local_times[0].isoformat(),
                "last_observation_local": local_times[-1].isoformat(),
            }
        )
        counters["accepted_complete_daily_mean"] += 1
    return rows, counters


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != TIMESERIES_COLUMNS:
            raise RuntimeError(f"Unexpected columns in {path}: {reader.fieldnames}")
        return list(reader)


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def write_static_metadata() -> None:
    metadata_path = METADATA_DIR / "luxembourg_age_reservoirs.csv"
    quality_path = METADATA_DIR / "luxembourg_age_quality_periods.csv"
    if not metadata_path.exists():
        write_csv(metadata_path, [METADATA_ROW], METADATA_COLUMNS)
    if not quality_path.exists():
        write_csv(quality_path, [QUALITY_ROW], QUALITY_COLUMNS)


def merge_daily_rows(
    old_rows: list[dict[str, str]], new_rows: list[dict[str, str]]
) -> tuple[list[dict[str, str]], list[str], list[str]]:
    merged = {row["date"]: row for row in old_rows}
    new_dates: list[str] = []
    revised_dates: list[str] = []
    for row in new_rows:
        row_date = row["date"]
        previous = merged.get(row_date)
        if previous is None:
            new_dates.append(row_date)
        elif previous != row:
            revised_dates.append(row_date)
        merged[row_date] = row
    return [merged[key] for key in sorted(merged)], new_dates, revised_dates


def validate_source(graph: dict[str, Any]) -> None:
    options = graph.get("options", {})
    if options.get("stationNumberTrimmed") != "40":
        raise RuntimeError("AGE response is no longer station 40")
    if options.get("waterLevelUnit") != "MetersOverSeaLevel":
        raise RuntimeError("AGE station 40 no longer reports metres over sea level")
    if not isinstance(graph.get("levels"), list) or not graph["levels"]:
        raise RuntimeError("AGE response contains no level observations")


def save_raw(payload: bytes, retrieved_at: datetime) -> Path:
    path = RAW_DIR / f"station_40_window_{retrieved_at.date().isoformat()}.json.gz"
    with gzip.open(path, "wb", compresslevel=9) as handle:
        handle.write(payload)
    return path


def run(max_lag_days: int) -> dict[str, Any]:
    ensure_dirs()
    write_static_metadata()
    retrieved_at = datetime.now(timezone.utc)
    payload, graph = fetch_json(GRAPH_API_URL)
    validate_source(graph)
    raw_path = save_raw(payload, retrieved_at)

    current_rows, counters = build_complete_daily_rows(graph["levels"])
    if not current_rows:
        raise RuntimeError("AGE rolling window yielded no complete Luxembourg local day")

    daily_path = DAILY_DIR / f"{RESERVOIR_ID}.csv"
    existing_rows = read_csv(daily_path)
    merged_rows, new_dates, revised_dates = merge_daily_rows(existing_rows, current_rows)
    write_csv(daily_path, merged_rows, TIMESERIES_COLUMNS)

    latest_date = date.fromisoformat(merged_rows[-1]["date"])
    local_today = retrieved_at.astimezone(LOCAL_TZ).date()
    lag_days = (local_today - latest_date).days
    if lag_days > max_lag_days:
        raise RuntimeError(
            f"Latest complete day {latest_date} is {lag_days} days old; "
            f"maximum allowed is {max_lag_days}"
        )

    summary: dict[str, Any] = {
        "retrieved_at_utc": retrieved_at.isoformat(),
        "source_api": GRAPH_API_URL,
        "reservoir_id": RESERVOIR_ID,
        "source_points": counters["source_points"],
        "complete_days_in_source_window": counters["accepted_complete_daily_mean"],
        "new_dates": new_dates,
        "revised_dates": revised_dates,
        "accumulated_rows": len(merged_rows),
        "coverage_start": merged_rows[0]["date"],
        "coverage_end": merged_rows[-1]["date"],
        "latest_complete_day_lag": lag_days,
        "max_lag_days": max_lag_days,
        "counters": dict(counters),
        "raw_file": str(raw_path.relative_to(OUTPUT_DIR)),
        "timeseries_file": str(daily_path.relative_to(OUTPUT_DIR)),
    }
    stamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-lag-days",
        type=int,
        default=3,
        help="Fail when the newest complete local day is older than this threshold.",
    )
    args = parser.parse_args()
    try:
        run(args.max_lag_days)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

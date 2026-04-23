from __future__ import annotations

import csv
import json
import os
import re
import sys
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


LIST_URL = "https://opengov.jp/en/geo/dam-reservoir/"
SOURCE_NAME = "MLIT Hydro DB (via OpenGov.jp)"
TIMEOUT = 30
JAPAN_TZ = timezone(timedelta(hours=9))

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
    "capacity_total (千m^3)",
    "dead_storage (千m^3)",
    "frl (m)",
    "dam_height (m)",
    "year_built",
    "main_use",
    "source_agency",
    "source_url",
    "last_updated",
    "detail_slug",
    "location",
    "data_period_start",
    "data_period_end",
    "data_period_days",
    "last_updated_source",
    "list_storage_rate_current (%)",
    "list_storage_volume_current (千m^3)",
    "list_7d_change_current (pt)",
]

SNAPSHOT_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "Storage Rate (%)",
    "Storage Volume (千m^3)",
    "Inflow (m^3/s)",
    "Outflow (m^3/s)",
]


@dataclass
class ListRecord:
    reservoir_id: str
    reservoir_name: str
    reservoir_name_en: str
    admin_unit: str
    basin: str
    detail_url: str
    detail_slug: str
    list_storage_rate_current: str
    list_storage_volume_current: str
    list_7d_change_current: str


def now_stamp() -> str:
    return datetime.now(JAPAN_TZ).strftime("%Y-%m-%d %H:%M:%S")


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


def extract_last_updated_source(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    match = re.search(r"Last updated:\s*(\d{4}-\d{2}-\d{2})", text)
    return match.group(1) if match else ""


def fetch_html(url: str, session: requests.Session) -> str:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.text


def write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def slugify_path(path: str) -> str:
    slug = path.rstrip("/").split("/")[-1]
    return slug or "unknown"


def parse_list_page(html: str) -> tuple[list[ListRecord], str]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Could not find dam list table on list page.")

    rows: list[ListRecord] = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue

        link = cells[1].find("a", href=True)
        if link is None:
            continue

        detail_url = urljoin(LIST_URL, link["href"])
        detail_slug = slugify_path(link["href"])
        reservoir_name = clean_text(link.get_text(" ", strip=True))
        admin_unit = ""
        prefecture_span = cells[1].find("span")
        if prefecture_span:
            admin_unit = clean_text(prefecture_span.get_text(" ", strip=True))

        rows.append(
            ListRecord(
                reservoir_id=f"JP_OPENGOV_{detail_slug}",
                reservoir_name=reservoir_name,
                reservoir_name_en=reservoir_name,
                admin_unit=admin_unit,
                basin=clean_cell(cells[2].get_text(" ", strip=True)),
                detail_url=detail_url,
                detail_slug=detail_slug,
                list_storage_rate_current=parse_number_text(cells[3].get_text(" ", strip=True)),
                list_storage_volume_current=parse_number_text(cells[4].get_text(" ", strip=True)),
                list_7d_change_current=parse_number_text(cells[5].get_text(" ", strip=True)),
            )
        )

    return rows, extract_last_updated_source(soup)


def parse_definition_list(soup: BeautifulSoup) -> dict[str, str]:
    result: dict[str, str] = {}
    heading = next(
        (h for h in soup.find_all(["h2", "h3"]) if clean_text(h.get_text()) == "Dam Information"),
        None,
    )
    if heading is None:
        return result

    block = heading.find_next("dl")
    if block is None:
        return result

    current_key = ""
    for node in block.find_all(["dt", "dd"], recursive=True):
        text = clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        if node.name == "dt":
            current_key = text
        elif current_key:
            result[current_key] = text
    return result


def find_chart_data(soup: BeautifulSoup, chart_type: str) -> dict[str, Any] | None:
    for canvas in soup.find_all("canvas"):
        if canvas.get("data-chart-type") != chart_type:
            continue
        raw = canvas.get("data-chart-data")
        if not raw:
            continue
        return json.loads(raw)
    return None


def split_water_system_and_river(value: str) -> tuple[str, str]:
    parts = [clean_cell(part) for part in value.split("/")]
    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def parse_data_period(value: str) -> tuple[str, str, str]:
    match = re.search(
        r"(\d{4}-\d{2}-\d{2})\s*〜\s*(\d{4}-\d{2}-\d{2})\s*\(([\d,]+)\s+days\)",
        value,
    )
    if not match:
        return "", "", ""
    start, end, days = match.groups()
    return start, end, days.replace(",", "")


def parse_recent_flow_table(
    soup: BeautifulSoup, reservoir_id: str, reservoir_name: str
) -> dict[str, dict[str, str]]:
    table = next(
        (
            candidate
            for candidate in soup.find_all("table")
            if [clean_text(th.get_text(" ", strip=True)) for th in candidate.find_all("th")[:5]]
            == ["Date", "Storage Rate", "Storage Volume", "Inflow", "Outflow"]
        ),
        None,
    )
    if table is None:
        return {}

    lookup: dict[str, dict[str, str]] = {}
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue
        day = clean_cell(cells[0].get_text(" ", strip=True))
        if not day:
            continue
        lookup[day] = {
            "reservoir_id": reservoir_id,
            "reservoir_name": reservoir_name,
            "date": day,
            "Storage Rate (%)": parse_number_text(cells[1].get_text(" ", strip=True)),
            "Storage Volume (千m^3)": parse_number_text(cells[2].get_text(" ", strip=True)),
            "Inflow (m^3/s)": parse_number_text(cells[3].get_text(" ", strip=True)),
            "Outflow (m^3/s)": parse_number_text(cells[4].get_text(" ", strip=True)),
        }
    return lookup


def dataset_by_label(chart_data: dict[str, Any], label: str) -> list[Any]:
    for dataset in chart_data.get("datasets", []):
        if dataset.get("label") == label:
            return dataset.get("data", [])
    return []


def parse_long_history_rows(
    soup: BeautifulSoup, reservoir_id: str, reservoir_name: str
) -> list[dict[str, str]]:
    rate_chart = find_chart_data(soup, "dam-longterm")
    storage_chart = find_chart_data(soup, "dam-storage")
    if not rate_chart or not storage_chart:
        return []

    rate_labels = rate_chart.get("labels", [])
    storage_labels = storage_chart.get("labels", [])
    if rate_labels != storage_labels:
        raise RuntimeError("Rate and storage chart labels do not match.")

    rate_values = dataset_by_label(rate_chart, "Storage Rate")
    storage_values = dataset_by_label(storage_chart, "Storage Volume")
    flow_lookup = parse_recent_flow_table(soup, reservoir_id, reservoir_name)

    rows: list[dict[str, str]] = []
    for index, day in enumerate(rate_labels):
        rate_value = rate_values[index] if index < len(rate_values) else ""
        storage_value = storage_values[index] if index < len(storage_values) else ""
        flows = flow_lookup.get(day, {})
        rows.append(
            {
                "reservoir_id": reservoir_id,
                "reservoir_name": reservoir_name,
                "date": day,
                "Storage Rate (%)": "" if rate_value in (None, "") else str(rate_value),
                "Storage Volume (千m^3)": "" if storage_value in (None, "") else str(storage_value),
                "Inflow (m^3/s)": flows.get("Inflow (m^3/s)", ""),
                "Outflow (m^3/s)": flows.get("Outflow (m^3/s)", ""),
            }
        )
    return rows


def build_metadata_row(
    item: ListRecord,
    info: dict[str, str],
    fetched_at: str,
    last_updated_source: str,
) -> dict[str, str]:
    basin, river = split_water_system_and_river(info.get("Water System / River", item.basin))
    data_start, data_end, data_days = parse_data_period(info.get("Data Period", ""))
    purposes = info.get("Purposes", "")

    return {
        "reservoir_id": item.reservoir_id,
        "reservoir_name": item.reservoir_name,
        "reservoir_name_en": item.reservoir_name_en,
        "country": "Japan",
        "admin_unit": item.admin_unit,
        "river": river,
        "basin": basin or item.basin,
        "lat": "",
        "lon": "",
        "capacity_total (千m^3)": parse_number_text(info.get("Effective Storage Capacity", "")),
        "dead_storage (千m^3)": "",
        "frl (m)": "",
        "dam_height (m)": "",
        "year_built": "",
        "main_use": purposes,
        "source_agency": SOURCE_NAME,
        "source_url": item.detail_url,
        "last_updated": fetched_at,
        "detail_slug": item.detail_slug,
        "location": info.get("Location", ""),
        "data_period_start": data_start,
        "data_period_end": data_end,
        "data_period_days": data_days,
        "last_updated_source": last_updated_source,
        "list_storage_rate_current (%)": item.list_storage_rate_current,
        "list_storage_volume_current (千m^3)": item.list_storage_volume_current,
        "list_7d_change_current (pt)": item.list_7d_change_current,
    }


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def target_dates() -> tuple[str, str]:
    start = os.environ.get("JAPAN_START_DATE")
    end = os.environ.get("JAPAN_END_DATE")
    today = datetime.now(JAPAN_TZ).date()

    if start or end:
        start_d = parse_date(start or end)
        end_d = parse_date(end or start)
    else:
        start_d = today
        end_d = today

    if end_d < start_d:
        raise ValueError("JAPAN_END_DATE must be >= JAPAN_START_DATE")
    return start_d.isoformat(), end_d.isoformat()


def within_range(day: str, start_date: str, end_date: str) -> bool:
    return start_date <= day <= end_date


def ensure_dirs(base: Path) -> dict[str, Path]:
    dirs = {
        "metadata": base / "metadata",
        "daily": base / "timeseries" / "daily",
        "run_logs": base / "run_logs",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def main() -> int:
    output_dir = Path(os.environ.get("OUTPUT_DIR", Path("data/japan/opengov"))).resolve()
    dirs = ensure_dirs(output_dir)
    start_date, end_date = target_dates()
    fetched_at = now_stamp()
    run_stamp = datetime.now(JAPAN_TZ).strftime("%Y%m%d_%H%M%S")
    summary_path = dirs["run_logs"] / f"{run_stamp}_summary.json"

    summary: dict[str, Any] = {
        "run_time": fetched_at,
        "source_list_url": LIST_URL,
        "status": "started",
        "start_date": start_date,
        "end_date": end_date,
        "metadata_file": str(dirs["metadata"] / "japan_opengov_reservoirs.csv"),
        "snapshot_files": [],
        "errors": [],
    }

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    try:
        list_html = fetch_html(LIST_URL, session)
        list_records, list_last_updated = parse_list_page(list_html)

        metadata_rows: list[dict[str, str]] = []
        daily_by_date: dict[str, list[dict[str, str]]] = defaultdict(list)

        for index, item in enumerate(list_records, start=1):
            print(f"[{index}/{len(list_records)}] {item.reservoir_name}  {item.detail_url}")
            try:
                detail_html = fetch_html(item.detail_url, session)
                soup = BeautifulSoup(detail_html, "html.parser")
                info = parse_definition_list(soup)
                last_updated_source = extract_last_updated_source(soup) or list_last_updated

                metadata_rows.append(
                    build_metadata_row(item, info, fetched_at, last_updated_source)
                )

                for row in parse_long_history_rows(soup, item.reservoir_id, item.reservoir_name):
                    if within_range(row["date"], start_date, end_date):
                        daily_by_date[row["date"]].append(row)
            except Exception as exc:
                summary["errors"].append(
                    {
                        "reservoir_id": item.reservoir_id,
                        "detail_url": item.detail_url,
                        "error": str(exc),
                    }
                )

        metadata_rows.sort(key=lambda row: row["reservoir_id"])
        write_csv(
            dirs["metadata"] / "japan_opengov_reservoirs.csv",
            METADATA_COLUMNS,
            metadata_rows,
        )

        snapshot_files: list[str] = []
        for snapshot_date in sorted(daily_by_date):
            rows = sorted(daily_by_date[snapshot_date], key=lambda row: row["reservoir_id"])
            path = dirs["daily"] / f"japan_opengov_timeseries_{snapshot_date}.csv"
            write_csv(path, SNAPSHOT_COLUMNS, rows)
            snapshot_files.append(str(path))

        summary.update(
            {
                "status": "success",
                "reservoir_count": len(metadata_rows),
                "date_count": len(daily_by_date),
                "row_count": sum(len(rows) for rows in daily_by_date.values()),
                "snapshot_files": snapshot_files,
                "list_last_updated": list_last_updated,
            }
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[DONE] metadata={len(metadata_rows)} dates={len(daily_by_date)} rows={summary['row_count']}"
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

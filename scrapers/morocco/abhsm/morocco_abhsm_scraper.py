"""Morocco ABHSM Souss-Massa barrage daily snapshot scraper.

Source:
- Official current barrage situation PDF:
  https://www.abhsm.ma/document/Remplissage_barrage/remplissage_barrage.pdf

Coverage:
- 9 dams in the Souss-Massa basin authority (ABHSM)

This source is ephemeral: it exposes the current snapshot PDF, which may later be
overwritten by a newer file. So the scraper stores both:
- a daily snapshot CSV
- the original PDF under raw/
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

import pdfplumber
import requests


BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "morocco_abhsm_outputs")

METADATA_DIR = OUTPUT_DIR / "metadata"
DAILY_DIR = OUTPUT_DIR / "timeseries" / "daily"
RAW_DIR = OUTPUT_DIR / "raw"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

PDF_URL = "https://www.abhsm.ma/document/Remplissage_barrage/remplissage_barrage.pdf"
SOURCE_PAGE_URL = "https://www.abhsm.ma/index.php/partenariat-5/situation-des-barrages"
SOURCE_AGENCY = "ABHSM"
TIMEOUT = 60

DAMS = [
    {
        "reservoir_id": "MA_ABHSM_YOUSSEF_BEN_TACHFINE",
        "reservoir_name": "Youssef Ben Tachfine",
        "reservoir_name_en": "Youssef Ben Tachfine",
        "lat": "29.802642",
        "lon": "-9.446361",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_ABDELMOUMEN",
        "reservoir_name": "Abdelmoumen",
        "reservoir_name_en": "Abdelmoumen",
        "lat": "30.765455",
        "lon": "-9.141891",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_AOULOUZ",
        "reservoir_name": "Aoulouz",
        "reservoir_name_en": "Aoulouz",
        "lat": "30.712758",
        "lon": "-8.091408",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_IMI_EL_KHENG",
        "reservoir_name": "Imi El Kheng",
        "reservoir_name_en": "Imi El Kheng",
        "lat": "30.720051",
        "lon": "-8.553662",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_MOKHTAR_SOUSSI",
        "reservoir_name": "Mokhtar Soussi",
        "reservoir_name_en": "Mokhtar Soussi",
        "lat": "30.726543",
        "lon": "-7.974982",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_PRINCE_MOULAY_ABDELLAH",
        "reservoir_name": "Prince Moulay Abdellah",
        "reservoir_name_en": "Prince Moulay Abdellah",
        "lat": "30.759593",
        "lon": "-9.688630",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_AHL_SOUSS",
        "reservoir_name": "Ahl Souss",
        "reservoir_name_en": "Ahl Souss",
        "lat": "30.052702",
        "lon": "-9.118332",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_SIDI_ABDELLAH",
        "reservoir_name": "Sidi Abdellah",
        "reservoir_name_en": "Sidi Abdellah",
        "lat": "30.633681",
        "lon": "-8.824358",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
    {
        "reservoir_id": "MA_ABHSM_DKHILA",
        "reservoir_name": "Dkhila",
        "reservoir_name_en": "Dkhila",
        "lat": "30.549120",
        "lon": "-9.288998",
        "admin_unit": "Souss-Massa",
        "river": "",
        "basin": "Souss-Massa",
    },
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
    "source_agency",
    "source_url",
    "data_type",
    "last_updated",
]

SNAPSHOT_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "Report timestamp (source local time)",
    "Capacité totale (Mm3)",
    "Volume Actuel (Mm3)",
    "T. de remplissage (%)",
    "Apports dernieres 24h (Mm3)",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dirs() -> None:
    for d in (METADATA_DIR, DAILY_DIR, RAW_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"[SAVE] {path} rows={len(rows)}")


def save_summary(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] {path}")


def clean_num(raw: str) -> str:
    raw = (raw or "").strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", raw)
    return m.group(0) if m else ""


def fetch_pdf(pdf_path: Path) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(PDF_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    pdf_path.write_bytes(r.content)
    print(f"[SAVE] {pdf_path}")


def extract_text(pdf_path: Path) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def parse_pdf(pdf_path: Path) -> tuple[str, list[dict]]:
    text = extract_text(pdf_path)
    lines = [re.sub(r"[\u202a-\u202e\u200e\u200f]", "", x).strip() for x in text.splitlines()]
    lines = [x for x in lines if x]

    date_line = next((x for x in lines if re.fullmatch(r"\d{2}/\d{2}/\d{4} à \d{1,2}h\d{2}", x)), "")
    if not date_line:
        raise RuntimeError("Could not find report timestamp in ABHSM PDF")
    report_dt = datetime.strptime(date_line, "%d/%m/%Y à %Hh%M")
    report_date = report_dt.strftime("%Y-%m-%d")

    rows: list[dict] = []
    for dam in DAMS:
        pattern = (
            re.escape(dam["reservoir_name"])
            + r"\s+"
            + r"(\d+,\d+)\s+(\d+,\d+)\s+(\d+,\d+)%\s+(\d+,\d+)"
        )
        m = re.search(pattern, text, flags=re.S | re.I)
        if not m:
            raise RuntimeError(f"Could not parse numeric row for {dam['reservoir_name']}")
        cap, vol, pct, inflow = m.groups()
        rows.append(
            {
                "reservoir_id": dam["reservoir_id"],
                "reservoir_name": dam["reservoir_name"],
                "date": report_date,
                "Report timestamp (source local time)": date_line,
                "Capacité totale (Mm3)": clean_num(cap),
                "Volume Actuel (Mm3)": clean_num(vol),
                "T. de remplissage (%)": clean_num(pct),
                "Apports dernieres 24h (Mm3)": clean_num(inflow),
            }
        )
    return report_date, rows


def build_metadata(fetched_at: str) -> list[dict]:
    rows = []
    for dam in DAMS:
        rows.append(
            {
                "reservoir_id": dam["reservoir_id"],
                "reservoir_name": dam["reservoir_name"],
                "reservoir_name_en": dam["reservoir_name_en"],
                "country": "Morocco",
                "admin_unit": dam["admin_unit"],
                "river": dam["river"],
                "basin": dam["basin"],
                "lat": dam["lat"],
                "lon": dam["lon"],
                "source_agency": SOURCE_AGENCY,
                "source_url": SOURCE_PAGE_URL,
                "data_type": "in_situ",
                "last_updated": fetched_at,
            }
        )
    return rows


def main() -> int:
    ensure_dirs()
    fetched_at = now_stamp()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    metadata_path = METADATA_DIR / "morocco_abhsm_reservoirs.csv"
    log_path = RUN_LOG_DIR / f"{stamp}_summary.json"

    summary = {
        "started_at": fetched_at,
        "source_agency": SOURCE_AGENCY,
        "source_url": PDF_URL,
        "output_dir": str(OUTPUT_DIR),
        "status": "started",
        "files": [],
        "errors": [],
    }

    try:
        temp_pdf = RAW_DIR / f"{stamp}_latest.pdf"
        fetch_pdf(temp_pdf)
        report_date, snapshot_rows = parse_pdf(temp_pdf)
        final_pdf = RAW_DIR / f"{report_date}_situation_des_barrages_souss_massa.pdf"
        if final_pdf.exists():
            temp_pdf.unlink()
        else:
            temp_pdf.rename(final_pdf)
        metadata_rows = build_metadata(fetched_at)
        daily_path = DAILY_DIR / f"morocco_abhsm_timeseries_{report_date}.csv"
        write_csv(metadata_path, METADATA_COLUMNS, metadata_rows)
        write_csv(daily_path, SNAPSHOT_COLUMNS, snapshot_rows)

        summary["status"] = "ok"
        summary["report_date"] = report_date
        summary["rows"] = len(snapshot_rows)
        summary["files"] = [str(metadata_path), str(daily_path), str(final_pdf)]
        save_summary(log_path, summary)
        return 0
    except Exception as e:
        summary["status"] = "error"
        summary["errors"].append({"message": str(e), "traceback": traceback.format_exc()})
        save_summary(log_path, summary)
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

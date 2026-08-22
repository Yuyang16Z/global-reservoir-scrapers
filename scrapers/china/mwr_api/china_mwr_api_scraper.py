#!/usr/bin/env python3
"""
China MWR large-reservoir realtime scraper without screenshot OCR.

The public page calls a JSON endpoint whose text values are wrapped like:
    #<font_id>otltag<fake_chars>#FontTag

The browser strips the wrapper and renders the fake characters with a custom
font.  This scraper uses the same public endpoint, infers the numeric glyphs
from encoded numeric fields, and trains the remaining Chinese text glyphs from
the repository's read-only historical OCR archive under
data/china/mwr_ocr_archive.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


PAGE_URL = "http://xxfb.mwr.cn/sq_dxsk.html?v=1.0"
API_URL = "http://xxfb.mwr.cn/OTMxbwsvgKjspwi/OTMbmdvbjQhky"
TZ = ZoneInfo("Asia/Shanghai")

TAG_RE = re.compile(r"#([A-Za-z0-9_]+)otltag([\s\S]*?)#FontTag")

TEXT_FIELD_PAIRS = [
    ("bsnm", "流域"),
    ("addvnm", "行政区划"),
    ("rvnm", "河名"),
    ("stnm", "库名"),
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
    "capacity_total (unit not published)",
    "dead_storage (unit not published)",
    "frl (m)",
    "dam_height (m)",
    "year_built",
    "main_use",
    "source_agency",
    "source_url",
    "data_type",
    "last_updated",
]

TIMESERIES_COLUMNS = [
    "reservoir_id",
    "reservoir_name",
    "date",
    "report_time",
    "basin",
    "admin_unit",
    "river",
    "库水位 (water level, m)",
    "日变幅 (daily water-level change, m)",
    "createTime",
    "decode_unresolved_count",
    "decode_unresolved_chars",
]

# Stable one-off corrections keyed by decoded source idNo. These cover rare
# glyphs that are underrepresented or missing in older OCR training history.
FIELD_OVERRIDES_BY_ID = {
    "12": {"stnm": "参窝水库"},
    "61": {"stnm": "鸳鸯池水库（坝上）"},
    "85": {"addvnm": "天津", "rvnm": "引滦明渠"},
    "137": {"stnm": "尤家卵"},
    "139": {"rvnm": "宏农涧河"},
    "154": {"rvnm": "玉符河", "stnm": "卧虎山水库"},
    "173": {"rvnm": "潍河", "stnm": "墙齐水库（东库）"},
    "174": {"rvnm": "潍河", "stnm": "墙齐水库（西库）"},
    "210": {"stnm": "常庄"},
    "233": {"stnm": "西苇水库"},
    "326": {"rvnm": "旬河", "stnm": "钟家坪"},
    "399": {"stnm": "短港"},
    "389": {"rvnm": "澴水支流，晏家河", "stnm": "芳畈"},
    "392": {"stnm": "滑石冲"},
    "419": {"bsnm": "长江"},
    "405": {"rvnm": "尧市", "stnm": "飞剑潭一坝水库"},
    "479": {"stnm": "锦江（仁化）"},
    "496": {"stnm": "七礤水库"},
    "568": {"stnm": "松涛水库（南丰）"},
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def split_tag(value: Any) -> tuple[str, str | None]:
    if value is None:
        return "", None
    if isinstance(value, (int, float)):
        return str(value), None
    text = str(value)
    match = TAG_RE.fullmatch(text)
    if match:
        return match.group(2), match.group(1)
    return text, None


def fetch_api_json(timeout_seconds: int, retries: int) -> dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Referer": PAGE_URL,
        "User-Agent": "Mozilla/5.0",
    }
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = requests.post(
                API_URL,
                headers=headers,
                data="null",
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("returncode") != 0 or not isinstance(payload.get("result"), list):
                raise RuntimeError(f"Unexpected API payload shape: {payload!r}")
            return payload
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                break
    raise RuntimeError(f"API request failed after {retries + 1} attempts: {last_exc}")


def infer_digit_map(api_rows: list[dict[str, Any]]) -> dict[str, str]:
    numeric_chars: list[str] = []
    for row in api_rows:
        for key in ("idNo", "rz", "rzRange"):
            text, font_id = split_tag(row.get(key, ""))
            if font_id:
                numeric_chars.extend(ch for ch in text if ord(ch) > 127)

    if not numeric_chars:
        raise RuntimeError("No encoded numeric characters found; cannot infer digit map.")

    counts = Counter(numeric_chars)
    codepoints = sorted({ord(ch) for ch in numeric_chars})
    candidates: list[tuple[int, int]] = []
    for cp in codepoints:
        block = [cp + 2 * i for i in range(10)]
        if all(item in codepoints for item in block):
            score = sum(counts[chr(item)] for item in block)
            candidates.append((score, cp))

    if not candidates:
        raise RuntimeError("Could not find a 10-glyph contiguous digit block.")

    _, base = max(candidates)
    return {chr(base + 2 * i): str(i) for i in range(10)}


def latest_history_paths(history_dir: Path, limit: int) -> list[Path]:
    full = sorted(history_dir.glob("*/mwr_ocr_full_table_*.csv"))
    table = sorted(history_dir.glob("*/mwr_ocr_table_*.csv"))
    paths = full or table
    return paths[-limit:]


def read_history_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    def sort_key(row: dict[str, str]) -> tuple[int, int]:
        try:
            screen = int(float(row.get("screen_index", "")))
        except ValueError:
            screen = 10**9
        try:
            row_order = int(float(row.get("row_order_in_screen", "")))
        except ValueError:
            row_order = 10**9
        return screen, row_order

    if rows and "screen_index" in rows[0] and "row_order_in_screen" in rows[0]:
        rows.sort(key=sort_key)
    return rows


def train_text_map(
    api_rows: list[dict[str, Any]],
    history_dir: Path,
    history_days: int,
    training_limit: int,
    digit_map: dict[str, str],
) -> tuple[dict[str, str], dict[str, Any]]:
    votes: dict[str, Counter[str]] = defaultdict(Counter)
    history_paths = latest_history_paths(history_dir, history_days)

    used_files = 0
    aligned_pairs = 0
    for path in history_paths:
        history_rows = read_history_csv(path)
        if not history_rows:
            continue
        used_files += 1
        n = min(len(history_rows), len(api_rows), training_limit)
        for idx in range(n):
            api_row = api_rows[idx]
            history_row = history_rows[idx]
            for api_key, history_key in TEXT_FIELD_PAIRS:
                encoded, _ = split_tag(api_row.get(api_key, ""))
                truth = (history_row.get(history_key) or "").strip()
                if not encoded or not truth or len(encoded) != len(truth):
                    continue
                aligned_pairs += 1
                for fake_ch, real_ch in zip(encoded, truth):
                    if fake_ch in digit_map or fake_ch in ".-+" or fake_ch.isspace():
                        continue
                    votes[fake_ch][real_ch] += 1

    char_map = {fake: counter.most_common(1)[0][0] for fake, counter in votes.items() if counter}
    conflict_count = sum(1 for counter in votes.values() if len(counter) > 1)
    diagnostics = {
        "history_dir": str(history_dir),
        "history_files_used": used_files,
        "history_files": [str(path) for path in history_paths],
        "aligned_field_pairs": aligned_pairs,
        "trained_chars": len(char_map),
        "conflicted_chars": conflict_count,
    }
    return char_map, diagnostics


def decode_tagged_value(
    value: Any,
    digit_map: dict[str, str],
    char_map: dict[str, str],
) -> tuple[str, Counter[str]]:
    text, font_id = split_tag(value)
    if not font_id:
        return text, Counter()

    unresolved: Counter[str] = Counter()
    out: list[str] = []
    for ch in text:
        if ch in digit_map:
            out.append(digit_map[ch])
        elif ch in char_map:
            out.append(char_map[ch])
        elif ch.isspace():
            continue
        elif ord(ch) <= 127:
            out.append(ch)
        else:
            out.append(ch)
            unresolved[ch] += 1
    return "".join(out), unresolved


def decode_source_rows(
    api_rows: list[dict[str, Any]],
    digit_map: dict[str, str],
    char_map: dict[str, str],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    decoded_rows: list[dict[str, str]] = []
    unresolved_global: Counter[str] = Counter()

    for source in api_rows:
        decoded_source: dict[str, str] = {}
        for key, value in source.items():
            decoded, _ = decode_tagged_value(value, digit_map, char_map)
            decoded_source[key] = decoded

        id_no = decoded_source.get("idNo", "")
        for key, replacement in FIELD_OVERRIDES_BY_ID.get(id_no, {}).items():
            decoded_source[key] = replacement

        row_unresolved = Counter()
        for key in ("bsnm", "addvnm", "rvnm", "stnm"):
            for ch in decoded_source.get(key, ""):
                if "\u3400" <= ch <= "\u4dbf":
                    row_unresolved[ch] += 1
        unresolved_global.update(row_unresolved)

        decoded_source["decode_unresolved_count"] = str(sum(row_unresolved.values()))
        decoded_source["decode_unresolved_chars"] = "".join(sorted(row_unresolved))
        decoded_rows.append(decoded_source)

    diagnostics = {
        "unresolved_unique_chars": len(unresolved_global),
        "unresolved_total_chars": sum(unresolved_global.values()),
        "unresolved_chars": {f"U+{ord(ch):04X}": count for ch, count in unresolved_global.items()},
    }
    return decoded_rows, diagnostics


def parse_report_datetime(report_time: str, now: datetime) -> tuple[str, str]:
    full_match = re.match(
        r"^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)$",
        report_time.strip(),
    )
    if full_match:
        year, month, day, hm = full_match.groups()
        return f"{year}-{month}-{day}", f"{year}-{month}-{day} {hm}"

    match = re.match(r"^(\d{2})-(\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)$", report_time.strip())
    if not match:
        today = now.strftime("%Y-%m-%d")
        return today, report_time

    month, day, hm = match.groups()
    year = now.year
    candidate = datetime(year, int(month), int(day), tzinfo=TZ)
    if (candidate.date() - now.date()).days > 7:
        year -= 1
    date = f"{year:04d}-{month}-{day}"
    return date, f"{date} {hm}"


def build_metadata_rows(decoded_rows: list[dict[str, str]], run_time: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in decoded_rows:
        rows.append(
            {
                "reservoir_id": row.get("idNo", ""),
                "reservoir_name": row.get("stnm", ""),
                "reservoir_name_en": "",
                "country": "China",
                "admin_unit": row.get("addvnm", ""),
                "river": row.get("rvnm", ""),
                "basin": row.get("bsnm", ""),
                "lat": row.get("lttd", ""),
                "lon": row.get("lgtd", ""),
                "capacity_total (unit not published)": "",
                "dead_storage (unit not published)": "",
                "frl (m)": "",
                "dam_height (m)": "",
                "year_built": "",
                "main_use": "",
                "source_agency": "MWR",
                "source_url": PAGE_URL,
                "data_type": "in_situ",
                "last_updated": run_time,
            }
        )
    return rows


def build_timeseries_rows(
    decoded_rows: list[dict[str, str]],
    report_date: str,
    report_datetime: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in decoded_rows:
        rows.append(
            {
                "reservoir_id": row.get("idNo", ""),
                "reservoir_name": row.get("stnm", ""),
                "date": report_date,
                "report_time": report_datetime,
                "basin": row.get("bsnm", ""),
                "admin_unit": row.get("addvnm", ""),
                "river": row.get("rvnm", ""),
                "库水位 (water level, m)": row.get("rz", ""),
                "日变幅 (daily water-level change, m)": row.get("rzRange", ""),
                "createTime": row.get("createTime", ""),
                "decode_unresolved_count": row.get("decode_unresolved_count", ""),
                "decode_unresolved_chars": row.get("decode_unresolved_chars", ""),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    root = repo_root()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=root / "data" / "china" / "mwr_ocr_archive",
        help="Existing OCR archive used as plaintext training data.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=root / "data" / "china" / "mwr_api",
        help="Output root for decoded API data.",
    )
    parser.add_argument("--history-days", type=int, default=18)
    parser.add_argument("--training-limit", type=int, default=568)
    parser.add_argument("--timeout-seconds", type=int, default=40)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()

    run_started = datetime.now(TZ)
    run_stamp = run_started.strftime("%Y%m%d_%H%M%S")
    run_time = run_started.strftime("%Y-%m-%d %H:%M:%S")

    payload = fetch_api_json(args.timeout_seconds, args.retries)
    api_rows = payload["result"]
    if not api_rows:
        raise RuntimeError("API returned zero rows.")

    digit_map = infer_digit_map(api_rows)
    char_map, train_diag = train_text_map(
        api_rows=api_rows,
        history_dir=args.history_dir,
        history_days=args.history_days,
        training_limit=args.training_limit,
        digit_map=digit_map,
    )
    decoded_rows, decode_diag = decode_source_rows(api_rows, digit_map, char_map)

    report_date, report_datetime = parse_report_datetime(
        decoded_rows[0].get("tm", ""),
        run_started,
    )
    output_dir = args.output_dir

    raw_path = output_dir / "raw" / f"china_mwr_api_raw_{report_date}_{run_stamp}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    metadata_rows = build_metadata_rows(decoded_rows, run_time)
    timeseries_rows = build_timeseries_rows(decoded_rows, report_date, report_datetime)

    metadata_path = output_dir / "metadata" / "china_mwr_api_reservoirs.csv"
    timeseries_path = output_dir / "timeseries" / "daily" / f"china_mwr_api_timeseries_{report_date}.csv"
    write_csv(metadata_path, metadata_rows, METADATA_COLUMNS)
    write_csv(timeseries_path, timeseries_rows, TIMESERIES_COLUMNS)

    diagnostics = {
        "run_started": run_time,
        "page_url": PAGE_URL,
        "api_url": API_URL,
        "row_count": len(api_rows),
        "report_date": report_date,
        "report_time": report_datetime,
        "digit_codepoints": {f"U+{ord(k):04X}": v for k, v in digit_map.items()},
        "manual_overrides_by_id": FIELD_OVERRIDES_BY_ID,
        "training": train_diag,
        "decoding": decode_diag,
        "raw_path": str(raw_path),
        "metadata_path": str(metadata_path),
        "timeseries_path": str(timeseries_path),
    }
    log_path = output_dir / "run_logs" / f"{run_stamp}_summary.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[china-mwr-api] rows={len(decoded_rows)} report_date={report_date}")
    print(f"[china-mwr-api] timeseries={timeseries_path}")
    print(f"[china-mwr-api] metadata={metadata_path}")
    print(f"[china-mwr-api] diagnostics={log_path}")
    if decode_diag["unresolved_total_chars"]:
        print(f"[china-mwr-api] WARN unresolved={decode_diag['unresolved_chars']}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[china-mwr-api] FAIL: {exc}", file=sys.stderr)
        raise

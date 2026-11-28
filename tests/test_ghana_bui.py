from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bui = load_module("ghana_bui_scraper", "scrapers/ghana/bui/ghana_bui_scraper.py")

ROW = {"date": "2026-09-21", "reservoir_level_masl": 171.43,
       "data_status": "Validated", "publication_status": "Published"}


def workbook(latest: dict) -> dict:
    """The Operations Centre answer of 2026-11-09 on: rows end 2026-09-21."""
    return {"meta": {"generated_at": "2026-11-09T09:22:38+00:00",
                     "latest_complete_date": "2026-09-21",
                     "latest_public_row_date": "2026-11-05"},
            "latest": latest,
            "rows": [dict(ROW, date="2026-09-20", reservoir_level_masl=171.34), ROW]}


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self.content = json.dumps(payload).encode() if payload is not None else b""


class LatestBlockTest(unittest.TestCase):
    def test_latest_level_is_read_under_reservoir_date(self):
        row = bui.ops_latest_row(workbook({"date": "2026-09-21", "reservoir_level_masl": 175.88,
                                           "reservoir_date": "2026-11-05"}), "t")
        self.assertEqual(row["measurement_date"], "2026-11-05")
        self.assertEqual(row["water_level_masl"], "175.88")
        self.assertEqual(row["source_array"], "operations_dashboard_latest")

    def test_missing_date_or_placeholder_level_gives_nothing(self):
        self.assertIsNone(bui.ops_latest_row(workbook({"reservoir_level_masl": 175.88}), "t"))
        self.assertIsNone(bui.ops_latest_row(
            workbook({"reservoir_level_masl": 0, "reservoir_date": "2026-11-05"}), "t"))
        self.assertIsNone(bui.ops_latest_row(
            workbook({"reservoir_level_masl": None, "reservoir_date": "2026-11-05"}), "t"))
        self.assertIsNone(bui.ops_latest_row({"rows": []}, "t"))


class MainMergeTest(unittest.TestCase):
    def run_main(self, payload: dict):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)

        def fake_get(url, **_kwargs):
            if url == bui.OPS_URL:
                return FakeResponse(200, payload)
            return FakeResponse(404)

        with mock.patch.multiple(bui, RAW_DIR=out / "raw", TS_DIR=out / "timeseries",
                                 RUN_LOG_DIR=out / "run_logs"), \
                mock.patch.object(bui.requests, "get", side_effect=fake_get), \
                mock.patch.object(bui.time, "sleep"):
            code = bui.main()
        log = json.loads(sorted((out / "run_logs").glob("*.json"))[0].read_text(encoding="utf-8"))
        lines = (out / "timeseries" / "ghana_bui_daily.csv").read_text(encoding="utf-8").splitlines()
        return code, log, lines

    def test_reading_ahead_of_the_rows_is_archived(self):
        code, log, lines = self.run_main(workbook(
            {"date": "2026-09-21", "reservoir_level_masl": 175.88, "reservoir_date": "2026-11-05"}))
        self.assertEqual(code, 0)
        self.assertEqual(log["operations_latest_reservoir_date"], "2026-11-05")
        self.assertEqual(log["operations_generated_at"], "2026-11-09T09:22:38+00:00")
        self.assertEqual(log["date_range"], ["2026-09-20", "2026-11-05"])
        self.assertEqual(lines[-1].split(",")[:4],
                         ["2026-11-05", "175.88", "GH_BPA_BUI", "operations_dashboard_latest"])
        self.assertEqual(len(lines), 4)

    def test_a_row_for_the_same_date_wins(self):
        code, log, lines = self.run_main(workbook(
            {"date": "2026-09-21", "reservoir_level_masl": 171.5, "reservoir_date": "2026-09-21"}))
        self.assertEqual(code, 0)
        self.assertEqual(lines[-1].split(",")[:4],
                         ["2026-09-21", "171.43", "GH_BPA_BUI", "operations_dashboard"])
        self.assertEqual(len(lines), 3)


if __name__ == "__main__":
    unittest.main()

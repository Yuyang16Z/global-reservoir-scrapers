from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
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


opengov = load_module("japan_opengov_scraper", "scrapers/japan/opengov/japan_opengov_scraper.py")

DAYS = ["2026-10-29", "2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02", "2026-11-03"]
LIST_PAGE = """<p>Last updated: 2026-11-03</p><table><tbody><tr><td>1</td>
<td><a href="/en/geo/dam-reservoir/aha/">Aha</a><span>Okinawa</span></td><td>Fukuji</td>
<td>99.8%</td><td>14,251</td><td>+0.1</td></tr></tbody></table>"""


def chart(kind: str, label: str, values: list[float]) -> str:
    data = json.dumps({"labels": DAYS, "datasets": [{"label": label, "data": values}]})
    return f"<canvas data-chart-type='{kind}' data-chart-data='{data}'></canvas>"


# Trimmed opengov.jp detail page: the daily charts hold every day of the data period.
DETAIL_PAGE = ("<p>Last updated: 2026-11-03</p>"
               + chart("dam-longterm", "Storage Rate", [99.1, 99.2, 99.3, 99.4, 99.5, 99.6])
               + chart("dam-storage", "Storage Volume", [14100, 14200, 14300, 14400, 14500, 14600]))


class JapanAfternoon(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 11, 3, 16, 30, tzinfo=tz)


class OpengovGapTests(unittest.TestCase):
    """A day published after its own run (2026-11-01) was never stored."""

    def run_scraper(self, stored: list[str], env: dict[str, str]):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        daily = root / "timeseries" / "daily"
        daily.mkdir(parents=True)
        for day in stored:
            (daily / f"japan_opengov_timeseries_{day}.csv").write_text("kept\n", encoding="utf-8")
        pages = {opengov.LIST_URL: LIST_PAGE}
        with mock.patch.dict(os.environ, {"OUTPUT_DIR": str(root), "JAPAN_START_DATE": "",
                                          "JAPAN_END_DATE": "", **env}), \
                mock.patch.object(opengov, "datetime", JapanAfternoon), \
                mock.patch.object(opengov, "fetch_html",
                                  side_effect=lambda url, session: pages.get(url, DETAIL_PAGE)):
            self.assertEqual(opengov.main(), 0)
        summary = json.loads(next((root / "run_logs").glob("*_summary.json")).read_text(encoding="utf-8"))
        files = {p.stem.rsplit("_", 1)[-1]: p.read_text(encoding="utf-8") for p in daily.iterdir()}
        return summary, files

    def test_a_default_run_fills_missing_days_inside_the_archive_span(self):
        summary, files = self.run_scraper(["2026-10-30", "2026-11-02"], {})
        # 10-29 is before the archive began; 10-30 and 11-02 are kept as stored.
        self.assertEqual(sorted(files), ["2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02",
                                         "2026-11-03"])
        self.assertEqual((files["2026-10-30"], files["2026-11-02"]), ("kept\n", "kept\n"))
        self.assertIn("JP_OPENGOV_aha,Aha,2026-11-01,99.4,14400", files["2026-11-01"])
        self.assertEqual(summary["gap_dates"], ["2026-10-31", "2026-11-01"])

    def test_manual_dates_keep_their_exact_range(self):
        summary, files = self.run_scraper(["2026-10-30"], {"JAPAN_START_DATE": "2026-11-02",
                                                           "JAPAN_END_DATE": "2026-11-02"})
        self.assertEqual(sorted(files), ["2026-10-30", "2026-11-02"])
        self.assertEqual(summary["gap_dates"], [])


if __name__ == "__main__":
    unittest.main()

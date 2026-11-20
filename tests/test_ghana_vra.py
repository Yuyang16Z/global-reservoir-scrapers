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


vra = load_module("ghana_vra_scraper", "scrapers/ghana/vra/ghana_vra_scraper.py")


class FakeResponse:
    def __init__(self, status_code: int, content: bytes):
        self.status_code = status_code
        self.content = content


class RefusedPagesTest(unittest.TestCase):
    """vra.com answered every request with HTTP 403 from 2026-11-15 on."""

    def run_main(self, response: FakeResponse):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        with mock.patch.multiple(vra, RAW_DIR=out / "raw", TS_DIR=out / "timeseries",
                                 RUN_LOG_DIR=out / "run_logs"), \
                mock.patch.object(vra.requests, "get", return_value=response), \
                mock.patch.object(vra.time, "sleep"):
            code = vra.main()
        logs = sorted((out / "run_logs").glob("*_summary.json"))
        self.assertEqual(len(logs), 1)
        return code, json.loads(logs[0].read_text(encoding="utf-8")), out

    def test_403_fails_and_keeps_status_and_body(self):
        body = b"<html>403 Forbidden</html>"
        code, log, out = self.run_main(FakeResponse(403, body))
        self.assertEqual(code, 1)
        self.assertEqual(log["status"], "error")
        self.assertEqual(log["unavailable_pages"], ["facts", "home"])
        for slug in ("facts", "home"):
            failure = log["page_failures"][slug]
            self.assertEqual(failure["http_status"], 403)
            self.assertEqual(failure["bytes"], len(body))
            self.assertEqual((out / "raw" / failure["saved_body"]).read_bytes(), body)
        self.assertFalse((out / "timeseries").exists()
                         and any((out / "timeseries").iterdir()))

    def test_good_page_still_parses(self):
        page = (b"<html><body>" + b"<p>filler</p>" * 200 +
                b"Lake Level (Wednesday, November 11, 2026): 264.15 feet | "
                b"80.512 meters</body></html>")
        code, log, out = self.run_main(FakeResponse(200, page))
        self.assertEqual(code, 0)
        self.assertEqual(log["status"], "ok")
        self.assertNotIn("page_failures", log)
        self.assertEqual(log["date_range"], ["2026-11-11", "2026-11-11"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


abhsm = load_module(
    "morocco_abhsm_scraper",
    "scrapers/morocco/abhsm/morocco_abhsm_scraper.py",
)
taiwan = load_module(
    "taiwan_wra_scraper",
    "scrapers/taiwan/wra/taiwan_wra_scraper.py",
)
freshness = load_module(
    "monitor_source_freshness",
    "scripts/monitor_source_freshness.py",
)
capetown = load_module(
    "southafrica_capetown_wcwss_scraper",
    "scrapers/southafrica/capetown_wcwss/southafrica_capetown_wcwss_scraper.py",
)


class AbhsmTransportTests(unittest.TestCase):
    def test_pdf_payload_validation(self):
        abhsm.validate_pdf_payload(b"%PDF-" + b"x" * abhsm.PDF_MIN_BYTES)
        with self.assertRaisesRegex(RuntimeError, "too small"):
            abhsm.validate_pdf_payload(b"%PDF-short")
        with self.assertRaisesRegex(RuntimeError, "PDF signature"):
            abhsm.validate_pdf_payload(b"x" * (abhsm.PDF_MIN_BYTES + 1))

    def test_expired_certificate_uses_only_pinned_fallback(self):
        payload = b"%PDF-" + b"x" * abhsm.PDF_MIN_BYTES
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            abhsm.requests,
            "get",
            side_effect=requests.exceptions.SSLError("certificate has expired"),
        ), mock.patch.object(
            abhsm,
            "fetch_with_pinned_expired_certificate",
            return_value=payload,
        ) as pinned:
            target = Path(tmp) / "report.pdf"
            transport = abhsm.fetch_pdf(target)
            self.assertEqual(transport, "pinned_expired_certificate")
            self.assertEqual(target.read_bytes(), payload)
            pinned.assert_called_once_with()

    def test_other_tls_errors_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            abhsm.requests,
            "get",
            side_effect=requests.exceptions.SSLError("hostname mismatch"),
        ), mock.patch.object(abhsm, "fetch_with_pinned_expired_certificate") as pinned:
            with self.assertRaises(requests.exceptions.SSLError):
                abhsm.fetch_pdf(Path(tmp) / "report.pdf")
            pinned.assert_not_called()


class TaiwanFallbackTests(unittest.TestCase):
    def test_snapshot_keeps_dominant_source_date(self):
        rows = {
            "A": {"observation_time": "2026-08-26T07:00:00"},
            "B": {"observation_time": "2026-08-26T08:00:00"},
            "C": {"observation_time": "2026-08-25T08:00:00"},
        }
        date, selected, counts = taiwan.select_current_daily_snapshot(rows)
        self.assertEqual(date, "2026-08-26")
        self.assertEqual(set(selected), {"A", "B"})
        self.assertEqual(counts, {"2026-08-25": 1, "2026-08-26": 2})

    def test_archived_current_daily_backfill_is_source_dated_and_idempotent(self):
        payload = [
            {
                "reservoiridentifier": "A",
                "reservoirname": "Alpha",
                "datetime": "2026-08-26T07:00:00",
                "capacity": "12.5",
                "inflow": "2.0",
            },
            {
                "reservoiridentifier": "B",
                "reservoirname": "Beta",
                "datetime": "2026-08-26T08:00:00",
                "capacity": "8.5",
                "inflow": "1.0",
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            dirs = taiwan.ensure_dirs(Path(tmp))
            raw = dirs["raw"] / "current_daily_ops_2026-08-27.json"
            raw.write_text(json.dumps(payload), encoding="utf-8")

            recovered = taiwan.backfill_archived_current_daily(dirs, {}, {})
            self.assertEqual(len(recovered), 1)
            daily = dirs["daily"] / "taiwan_timeseries_2026-08-26.csv"
            with daily.open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual({row["date"] for row in rows}, {"2026-08-26"})
            self.assertEqual({row["reservoir_id"] for row in rows}, {"A", "B"})
            self.assertEqual(taiwan.backfill_archived_current_daily(dirs, {}, {}), [])


class CapeTownFallbackTests(unittest.TestCase):
    def test_official_media_endpoint_follows_primary_timeout(self):
        payload = b"%PDF-" + b"x" * capetown.PDF_MIN_BYTES
        response = mock.Mock(content=payload)
        response.raise_for_status.return_value = None
        with mock.patch.object(
            capetown.requests,
            "get",
            side_effect=[requests.ConnectionError("primary timeout"), response],
        ) as get:
            body, url = capetown.fetch_pdf()
        self.assertEqual(body, payload)
        self.assertEqual(url, capetown.FALLBACK_PDF_URL)
        self.assertEqual(get.call_count, 2)


class FreshnessComponentTests(unittest.TestCase):
    def test_components_are_monitored_independently(self):
        source = {
            "source_id": "taiwan/wra",
            "data_path": "data/taiwan/wra",
            "publication_cadence_hours": 24,
            "max_schedule_gap_hours": 192,
            "freshness_components": [
                {"name": "daily", "data_path": "data/taiwan/wra/timeseries/daily"},
                {"name": "intraday", "data_path": "data/taiwan/wra/timeseries/intraday"},
            ],
        }
        targets = freshness.freshness_targets(source)
        self.assertEqual(
            [target["source_id"] for target in targets],
            ["taiwan/wra:daily", "taiwan/wra:intraday"],
        )


if __name__ == "__main__":
    unittest.main()

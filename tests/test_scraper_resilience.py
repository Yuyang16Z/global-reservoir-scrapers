from __future__ import annotations

import csv
import importlib.util
import json
import sys
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
    sys.modules[name] = module
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
pagasa = load_module(
    "philippines_pagasa_scraper",
    "scrapers/philippines/pagasa/philippines_pagasa_scraper.py",
)
mwr = load_module(
    "china_mwr_api_scraper",
    "scrapers/china/mwr_api/china_mwr_api_scraper.py",
)
luxembourg = load_module(
    "luxembourg_age_scraper",
    "scrapers/luxembourg/age/luxembourg_age_scraper.py",
)


class ChinaMwrTransportTests(unittest.TestCase):
    def test_direct_official_api_is_preferred(self):
        payload = {"returncode": 0, "result": [{"idNo": "encoded"}]}
        response = mock.Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload
        with mock.patch.object(mwr.requests, "post", return_value=response), mock.patch.object(
            mwr.requests, "get"
        ) as relay:
            actual, diagnostics = mwr.fetch_api_json(10, 0)
        self.assertEqual(actual, payload)
        self.assertEqual(diagnostics["transport"], "direct_official_api")
        self.assertFalse(diagnostics["fallback_used"])
        relay.assert_not_called()

    def test_relay_recovers_direct_network_failure(self):
        payload = {"returncode": 0, "result": [{"idNo": "encoded"}]}
        response = mock.Mock(text="Title:\n\nMarkdown Content:\n" + json.dumps(payload))
        response.raise_for_status.return_value = None
        with mock.patch.object(
            mwr.requests,
            "post",
            side_effect=requests.ConnectionError("network is unreachable"),
        ), mock.patch.object(mwr.requests, "get", return_value=response):
            actual, diagnostics = mwr.fetch_api_json(10, 0)
        self.assertEqual(actual, payload)
        self.assertEqual(diagnostics["transport"], "jina_reader_relay")
        self.assertTrue(diagnostics["fallback_used"])
        self.assertIn("network is unreachable", diagnostics["prior_errors"][0])

    def test_doh_address_recovers_name_resolution_failure(self):
        # The 2026-09-17..22 evening failure: the runner cannot resolve the host.
        payload = {"returncode": 0, "result": [{"idNo": "encoded"}]}
        calls = []

        def post(url, headers, data, timeout):
            calls.append((url, headers.get("Host")))
            if url.startswith(f"http://{mwr.API_HOST}/"):
                raise requests.ConnectionError(
                    f"Failed to resolve '{mwr.API_HOST}' "
                    "([Errno -3] Temporary failure in name resolution)"
                )
            response = mock.Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = payload
            return response

        doh = mock.Mock()
        doh.raise_for_status.return_value = None
        doh.json.return_value = {"Status": 0, "Answer": [
            {"name": f"{mwr.API_HOST}.", "type": 5, "data": "cdn.example.cn."},
            {"name": "cdn.example.cn.", "type": 1, "data": "203.0.113.7"},
        ]}
        with mock.patch.object(mwr.requests, "post", side_effect=post), mock.patch.object(
            mwr.requests, "get", return_value=doh
        ) as get:
            actual, diagnostics = mwr.fetch_api_json(10, 0)
        self.assertEqual(actual, payload)
        self.assertEqual(diagnostics["transport"], "direct_official_api_doh_address")
        self.assertEqual(diagnostics["resolved_address"], "203.0.113.7")
        self.assertEqual(diagnostics["fetch_url"], mwr.API_URL)
        self.assertEqual(
            calls[-1],
            (mwr.API_URL.replace(mwr.API_HOST, "203.0.113.7", 1), mwr.API_HOST),
        )
        # One DoH lookup, no relay request.
        self.assertEqual(get.call_count, 1)
        self.assertEqual(get.call_args.args[0], mwr.DOH_RESOLVERS[0])

    def test_relay_still_follows_when_doh_cannot_resolve(self):
        payload = {"returncode": 0, "result": [{"idNo": "encoded"}]}
        no_answer = mock.Mock()
        no_answer.raise_for_status.return_value = None
        no_answer.json.return_value = {"Status": 2}
        relay = mock.Mock(text="Markdown Content:\n" + json.dumps(payload))
        relay.raise_for_status.return_value = None
        with mock.patch.object(
            mwr.requests,
            "post",
            side_effect=requests.ConnectionError("Temporary failure in name resolution"),
        ), mock.patch.object(
            mwr.requests,
            "get",
            side_effect=[no_answer] * len(mwr.DOH_RESOLVERS) + [relay],
        ):
            actual, diagnostics = mwr.fetch_api_json(10, 0)
        self.assertEqual(actual, payload)
        self.assertEqual(diagnostics["transport"], "jina_reader_relay")
        self.assertTrue(any("DoH lookup failed" in e for e in diagnostics["prior_errors"]))


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

    def test_existing_current_fallback_is_reported_as_archived(self):
        with tempfile.TemporaryDirectory() as tmp:
            daily_dir = Path(tmp)
            (daily_dir / "taiwan_timeseries_2026-08-26.csv").touch()
            status, note = taiwan.describe_current_daily_fallback(
                "2026-08-26", False, [], daily_dir
            )
        self.assertEqual(status, "already_archived")
        self.assertIn("2026-08-26 was already archived", note)


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


class PagasaFallbackTests(unittest.TestCase):
    def test_official_fallback_follows_primary_timeout(self):
        observation = pagasa.DailyObservation(
            dam_name="Angat",
            date="2026-08-27",
            observation_time="08:00 AM",
            rwl="192.83",
            wl_deviation="0.21",
            nhwl="210.00",
            dev_nhwl="-17.17",
            rule_curve="183.05",
            dev_rule_curve="9.78",
            gates="",
            meters="",
            inflow="",
            outflow="",
        )
        session = mock.Mock()
        with mock.patch.object(
            pagasa,
            "fetch_html",
            side_effect=[requests.ConnectTimeout("primary timeout"), "<html></html>"],
        ) as fetch, mock.patch.object(
            pagasa,
            "extract_page_timestamp",
            return_value=("August 27,2026 08:00:00 am", pagasa.date(2026, 8, 27)),
        ), mock.patch.object(
            pagasa,
            "find_dam_table",
            return_value=object(),
        ), mock.patch.object(
            pagasa,
            "parse_dam_table",
            return_value=[observation],
        ):
            _, url, _, page_date, observations = pagasa.fetch_pagasa_page(session)
        self.assertEqual(url, pagasa.FALLBACK_SOURCE_URL)
        self.assertEqual(page_date.isoformat(), "2026-08-27")
        self.assertEqual(observations, [observation])
        self.assertEqual(fetch.call_count, 2)


class FakeUrlopenResponse:
    def __init__(self, body: bytes, content_type: str):
        self.body = body
        self.status = 200
        self.headers = {"Content-Type": content_type}

    def read(self) -> bytes:
        return self.body

    def geturl(self) -> str:
        return luxembourg.GRAPH_API_URL

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> bool:
        return False


class LuxembourgNonJsonTests(unittest.TestCase):
    def fetch_failure(self, body: bytes, content_type: str) -> str:
        with mock.patch.object(
            luxembourg.urllib.request,
            "urlopen",
            return_value=FakeUrlopenResponse(body, content_type),
        ) as urlopen, mock.patch.object(luxembourg.time, "sleep"):
            with self.assertRaises(RuntimeError) as raised:
                luxembourg.fetch_json(luxembourg.GRAPH_API_URL, attempts=2)
        self.assertEqual(urlopen.call_count, 2)
        return str(raised.exception)

    def test_non_json_success_says_what_came_back(self):
        # The shape behind the 2026-09-15.. outage signature: one leading newline,
        # then a non-JSON token, e.g. a PHP warning printed ahead of the payload.
        message = self.fetch_failure(
            b'\n<br />\n<b>Warning</b>:  Undefined index in <b>api.php</b><br />\n{"levels": []}',
            "text/html; charset=UTF-8",
        )
        self.assertIn("Expecting value: line 2 column 1 (char 1)", message)
        self.assertIn(f"HTTP 200 from {luxembourg.GRAPH_API_URL}", message)
        self.assertIn("Content-Type='text/html; charset=UTF-8'", message)
        # Whitespace is collapsed so the excerpt stays on one log line.
        self.assertIn("body starts '<br /> <b>Warning</b>: Undefined index", message)

    def test_html_page_title_is_reported(self):
        message = self.fetch_failure(
            b"<!DOCTYPE html><html><head><title>\n  Maintenance\n</title></head></html>",
            "text/html",
        )
        self.assertIn("title='Maintenance'", message)

    def test_json_payload_still_parses(self):
        body = b'{"options": {"stationNumberTrimmed": "40"}, "levels": []}'
        with mock.patch.object(
            luxembourg.urllib.request,
            "urlopen",
            return_value=FakeUrlopenResponse(body, "application/json"),
        ):
            payload, graph = luxembourg.fetch_json(luxembourg.GRAPH_API_URL)
        self.assertEqual(payload, body)
        self.assertEqual(graph["options"]["stationNumberTrimmed"], "40")


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

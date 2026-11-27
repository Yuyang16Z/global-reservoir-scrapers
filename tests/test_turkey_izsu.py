from __future__ import annotations

import csv
import gzip
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


izsu = load_module("turkey_izsu_scraper", "scrapers/turkey/izsu/turkey_izsu_scraper.py")

TAHTALI = ("c7623aa5-5e0d-4fa5-5f6e-08de4e88add2", "Tahtalı Barajı")
GORDES = ("7bdb2e27-51ab-451c-5f78-08de4e88add2", "Gördes Barajı")
STATIC_CSV = (
    "﻿BARAJ_KUYU_ID;BARAJ_KUYU_ADI;DURUM_TARIHI;SU_DURUMU;DOLULUK_ORANI;SU_YUKSEKLIGI;"
    "TUKETILEBILIR_SU_KAPASITESI;MAKSIMUM_SU_KAPASITESI;MAKSIMUM_SU_YUKSEKLIGI;MINIMUM_SU_KAPASITESI;"
    "MINIMUM_SU_YUKSEKLIGI;BARAJ_SU_DURUMU_GOSTERIMI;SIRA;ENLEM;BOYLAM;KULLANILABILIR_GOL_SU_HACMI\r\n"
    "225;Tahtalı Barajı;2026-10-14T00:00:00;142323000;42.75;42.75;122723000;306650000;60.5;19600000;31.0;;11;"
    "38.157434;27.122630;287050000\r\n").encode("utf-8")


def record(day: str, facility: tuple[str, str], volume: str, rate: str = "40.32", level: str = "51.78") -> str:
    """One record exactly as the API writes it (numbers unquoted, volume with two decimals)."""
    return ('{"id":"%s-%s","date":"%s","waterStatus":%s,"usableWaterStatus":0,"occupancyRate":%s,'
            '"waterLevel":%s,"damWellFacility":{"id":"%s","name":"%s"}}'
            % (day, facility[0][:8], day, volume, rate, level, facility[0], facility[1]))


def answer(records: list[str]) -> bytes:
    return ('{"data":[%s],"statusCode":200,"ok":true,"errorMessage":null}' % ",".join(records)).encode("utf-8")


class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        self.content = content


class FakeApi:
    """Serves ?year= and ?date= from one table of records; `fail` maps a query to an HTTP status."""

    def __init__(self):
        self.days: dict[str, list[str]] = {}
        self.fail: dict[str, int] = {}
        self.static: bytes | None = STATIC_CSV
        self.calls: list[str] = []

    def put(self, day: str, volume: str = "135338000.00", facilities=(TAHTALI, GORDES)) -> None:
        self.days[day] = [record(day, f, volume) for f in facilities]

    def get(self, url, **_kwargs):
        if url == izsu.STATIC_CSV_URL:
            return FakeResponse(200, self.static) if self.static else FakeResponse(404)
        query = urlsplit(url).query
        self.calls.append(query)
        if query in self.fail:
            return FakeResponse(self.fail[query], b"refused")
        args = parse_qs(query)
        if "year" in args:
            days = sorted((d for d in self.days if d.startswith(args["year"][0] + "-")), reverse=True)
        else:
            days = [d for d in self.days if d == args["date"][0]]
        return FakeResponse(200, answer([r for d in days for r in self.days[d]]))


class ScraperCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.out = Path(tmp.name)
        self.api = FakeApi()
        self.clock = datetime(2026, 11, 27, 9, 0, 0, tzinfo=timezone.utc)
        patches = [
            mock.patch.multiple(izsu, OUTPUT_DIR=self.out, RAW_DIR=self.out / "raw", TS_DIR=self.out / "timeseries",
                                META_DIR=self.out / "metadata", RUN_LOG_DIR=self.out / "run_logs",
                                MAX_DATE_REQUESTS=3, REREAD_ALL_YEARS=False),
            mock.patch.object(izsu.requests, "get", side_effect=self.api.get),
            mock.patch.object(izsu.time, "sleep"),
            mock.patch.object(izsu, "now_utc", side_effect=self.tick),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def tick(self) -> datetime:
        self.clock += timedelta(seconds=1)
        return self.clock

    def run_main(self, at: datetime | None = None):
        if at:
            self.clock = at
        self.api.calls.clear()
        code = izsu.main()
        log = json.loads(sorted((self.out / "run_logs").glob("*.json"))[-1].read_text(encoding="utf-8"))
        return code, log

    def table(self, name: str) -> list[dict]:
        with (self.out / name).open(encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def raw_files(self) -> list[str]:
        return sorted(p.relative_to(self.out / "raw").as_posix() for p in (self.out / "raw").rglob("*")
                      if p.is_file() and p.name != "captures.csv")

    def seed(self) -> None:
        """Five dates in 2026, two in 2025, nothing earlier."""
        for day in ("2026-11-23", "2026-11-24", "2026-11-25", "2026-11-26", "2026-11-27"):
            self.api.put(day)
        self.api.put("2025-12-30", "19996000.00")
        self.api.put("2025-12-31", "19996000.00", facilities=(TAHTALI,))


class AnswerTests(unittest.TestCase):
    def test_numbers_keep_the_text_the_api_sent(self):
        days, problem = izsu.read_answer(answer([record("2026-11-27", TAHTALI, "135338000.00")]))
        self.assertEqual(problem, "")
        rec = days["2026-11-27"][0]
        self.assertEqual((rec["waterStatus"], rec["usableWaterStatus"], rec["occupancyRate"]),
                         ("135338000.00", "0", "40.32"))

    def test_unusable_answers_say_why(self):
        self.assertEqual(izsu.read_answer(b"<html>502</html>"), (None, "answer is not JSON"))
        self.assertEqual(izsu.read_answer(b'"`year` parametresi 1900 ile 2027 arasinda olmalidir."'),
                         (None, "answer is not a JSON object"))
        days, problem = izsu.read_answer(b'{"data":null,"statusCode":500,"ok":false,"errorMessage":"boom"}')
        self.assertIsNone(days)
        self.assertIn("statusCode 500: boom", problem)
        self.assertEqual(izsu.read_answer(b'{"data":[],"statusCode":200,"ok":true,"errorMessage":null}'), ({}, ""))

    def test_hash_ignores_record_id_and_order_but_not_values(self):
        a = json.loads(answer([record("2026-11-27", TAHTALI, "1.00"), record("2026-11-27", GORDES, "2.00")]),
                       parse_float=str, parse_int=str)["data"]
        b = [dict(a[1], id="another-id"), dict(a[0], id="yet-another")]
        self.assertEqual(izsu.data_hash(a), izsu.data_hash(b))
        self.assertNotEqual(izsu.data_hash(a), izsu.data_hash([dict(a[0], waterStatus="1.01"), a[1]]))


class FirstRunTests(ScraperCase):
    def test_reads_back_until_three_empty_years_and_stores_each_year_once(self):
        self.seed()
        code, log = self.run_main()
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertEqual(self.api.calls, ["year=2026", "year=2025", "year=2024", "year=2023", "year=2022"])
        years = [f for f in self.raw_files() if f.startswith("years/")]
        self.assertEqual([f[6:10] for f in years], ["2022", "2023", "2024", "2025", "2026"])
        self.assertEqual(log["years"], {"2025": {"dates": 2, "rows": 3}, "2026": {"dates": 5, "rows": 10}})
        self.assertEqual((log["earliest_record_date"], log["latest_record_date"]), ("2025-12-30", "2026-11-27"))
        rows = self.table("timeseries/turkey_izsu_2026.csv")
        self.assertEqual(rows[-1]["date"], "2026-11-27")
        self.assertEqual(rows[-1]["reservoir_name"], "Tahtalı Barajı")       # sorted by name within a date
        self.assertEqual(rows[-1]["reservoir_id"], TAHTALI[0])
        self.assertEqual(rows[-1]["waterStatus"], "135338000.00")
        self.assertEqual((rows[-1]["version"], rows[-1]["n_versions"]), ("1", "1"))
        self.assertTrue(rows[-1]["raw_file"].startswith("years/2026_"))
        self.assertEqual(self.table("timeseries/turkey_izsu_revisions.csv"), [])

    def test_a_stored_year_answer_is_the_bytes_as_served(self):
        self.seed()
        self.run_main()
        path = next((self.out / "raw" / "years").glob("2025_*.json.gz"))
        self.assertEqual(gzip.decompress(path.read_bytes()),
                         answer(self.api.days["2025-12-31"] + self.api.days["2025-12-30"]))
        captures = {c["scope"]: c for c in self.table("raw/captures.csv")}
        self.assertEqual((captures["2025"]["records"], captures["2025"]["dates"]), ("3", "2"))
        self.assertEqual((captures["2025"]["first_date"], captures["2025"]["last_date"]),
                         ("2025-12-30", "2025-12-31"))
        self.assertEqual(captures["2023"]["records"], "0")

    def test_metadata_takes_coordinates_and_capacities_from_the_static_csv(self):
        self.seed()
        _code, log = self.run_main()
        self.assertTrue(log["static_csv"].startswith("static/izsu-baraj-doluluk-oranlari_"))
        meta = {m["reservoir_name"]: m for m in self.table("metadata/turkey_izsu_reservoirs.csv")}
        self.assertEqual(set(meta), {"Tahtalı Barajı", "Gördes Barajı"})
        tahtali = meta["Tahtalı Barajı"]
        self.assertEqual((tahtali["lat"], tahtali["lon"]), ("38.157434", "27.122630"))
        self.assertEqual((tahtali["MAKSIMUM_SU_KAPASITESI"], tahtali["MINIMUM_SU_KAPASITESI"]),
                         ("306650000", "19600000"))
        self.assertEqual(tahtali["first_date"], "2025-12-30")
        self.assertEqual((meta["Gördes Barajı"]["lat"], meta["Gördes Barajı"]["static_file"]), ("", ""))

    def test_missing_static_csv_does_not_fail_the_run(self):
        self.seed()
        self.api.static = None
        code, log = self.run_main()
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertTrue(log["static_csv"].startswith("not usable"))
        self.assertEqual({m["lat"] for m in self.table("metadata/turkey_izsu_reservoirs.csv")}, {""})


class DailyRunTests(ScraperCase):
    def setUp(self):
        super().setUp()
        self.seed()
        self.run_main()

    def snapshot(self) -> dict[str, bytes]:
        return {p.relative_to(self.out).as_posix(): p.read_bytes() for p in self.out.rglob("*")
                if p.is_file() and "run_logs" not in p.parts}

    def test_unchanged_source_asks_for_the_current_year_only_and_stores_nothing(self):
        before = self.snapshot()
        code, log = self.run_main(datetime(2026, 11, 27, 21, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"], log["new_raw_files"]), (0, "ok", []))
        self.assertEqual(self.api.calls, ["year=2026"])
        self.assertEqual(log["year_requests"][0]["result"], "unchanged")
        self.assertEqual(self.snapshot(), before)

    def test_a_new_day_is_asked_for_on_its_own_and_stored_as_served(self):
        self.api.put("2026-11-28", "133000000.00")
        code, log = self.run_main(datetime(2026, 11, 28, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertEqual(self.api.calls, ["year=2026", "date=2026-11-28"])
        (name,) = log["new_raw_files"]
        self.assertRegex(name, r"^2026/20261128_20261128T\d{6}Z__[0-9a-f]{12}\.json$")
        self.assertEqual((self.out / "raw" / name).read_bytes(), answer(self.api.days["2026-11-28"]))
        rows = self.table("timeseries/turkey_izsu_2026.csv")
        self.assertEqual(len(rows), 12)
        self.assertEqual((rows[-1]["date"], rows[-1]["waterStatus"], rows[-1]["raw_file"]),
                         ("2026-11-28", "133000000.00", name))

    def test_a_changed_value_becomes_a_new_version_and_the_old_one_is_kept(self):
        self.api.put("2026-11-25", "140000000.00")
        code, log = self.run_main(datetime(2026, 11, 28, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"], log["dates_with_revisions"]), (0, "ok", ["2026-11-25"]))
        latest = [r for r in self.table("timeseries/turkey_izsu_2026.csv") if r["date"] == "2026-11-25"]
        self.assertEqual({(r["waterStatus"], r["version"], r["n_versions"]) for r in latest},
                         {("140000000.00", "2", "2")})
        revisions = self.table("timeseries/turkey_izsu_revisions.csv")
        self.assertEqual([(r["version"], r["waterStatus"]) for r in revisions if r["reservoir_id"] == TAHTALI[0]],
                         [("1", "135338000.00"), ("2", "140000000.00")])
        # the same answer again changes nothing
        _code, log = self.run_main(datetime(2026, 11, 28, 21, 0, tzinfo=timezone.utc))
        self.assertEqual((log["new_raw_files"], self.api.calls), ([], ["year=2026"]))

    def test_a_long_gap_stores_the_year_answer_instead_of_many_date_answers(self):
        for day in ("2026-11-28", "2026-11-29", "2026-11-30", "2026-12-01"):       # more than MAX_DATE_REQUESTS
            self.api.put(day)
        code, log = self.run_main(datetime(2026, 12, 1, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"], self.api.calls), (0, "ok", ["year=2026"]))
        self.assertRegex(log["new_raw_files"][0], r"^years/2026_20261201T")
        self.assertEqual(log["years"]["2026"], {"dates": 9, "rows": 18})

    def test_a_date_the_api_stops_listing_is_reported_and_kept(self):
        del self.api.days["2026-11-24"]
        code, log = self.run_main(datetime(2026, 11, 28, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"], log["dates_no_longer_served"]), (0, "partial", ["2026-11-24"]))
        self.assertIn("2026-11-24", {r["date"] for r in self.table("timeseries/turkey_izsu_2026.csv")})

    def test_january_also_compares_the_previous_year(self):
        self.api.put("2026-12-31")
        self.api.put("2027-01-01")
        code, log = self.run_main(datetime(2027, 1, 1, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertEqual(self.api.calls, ["year=2027", "date=2027-01-01", "year=2026", "date=2026-12-31"])
        self.assertEqual(self.table("timeseries/turkey_izsu_2027.csv")[0]["date"], "2027-01-01")

    def test_a_finished_year_gets_its_own_stored_answer_once(self):
        self.api.put("2027-02-01")
        self.run_main(datetime(2027, 2, 1, 9, 0, tzinfo=timezone.utc))
        self.assertEqual(self.api.calls, ["year=2027", "date=2027-02-01"])       # 2026 has a stored answer already
        self.api.put("2028-02-01")
        code, log = self.run_main(datetime(2028, 2, 1, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertEqual(self.api.calls, ["year=2028", "date=2028-02-01", "year=2027"])
        self.assertEqual(len(list((self.out / "raw" / "years").glob("2027_*.json.gz"))), 1)
        self.run_main(datetime(2028, 2, 2, 9, 0, tzinfo=timezone.utc))
        self.assertEqual(self.api.calls, ["year=2028"])

    def test_reread_all_years_stores_only_an_answer_that_differs(self):
        self.api.put("2025-12-30", "20000000.00")
        with mock.patch.object(izsu, "REREAD_ALL_YEARS", True):
            code, log = self.run_main(datetime(2026, 11, 28, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertEqual(self.api.calls, ["year=2026", "year=2025", "year=2024", "year=2023", "year=2022"])
        self.assertEqual(len(log["new_raw_files"]), 1)
        self.assertRegex(log["new_raw_files"][0], r"^years/2025_20261128T")
        self.assertEqual(log["dates_with_revisions"], ["2025-12-30"])

    def test_a_record_dated_after_its_capture_is_reported_and_not_kept(self):
        self.api.put("2026-12-25")
        code, log = self.run_main(datetime(2026, 11, 28, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"]), (0, "partial"))
        self.assertEqual(len(log["dates_after_capture"]), 1)
        self.assertNotIn("2026-12-25", {r["date"] for r in self.table("timeseries/turkey_izsu_2026.csv")})
        self.assertEqual(log["latest_record_date"], "2026-11-27")


class FailureTests(ScraperCase):
    def test_no_answer_for_the_current_year_fails_and_stores_nothing(self):
        self.seed()
        self.api.fail["year=2026"] = 502
        code, log = self.run_main()
        self.assertEqual((code, log["status"], log["blocked"]), (1, "failed", False))
        self.assertEqual(self.api.calls, ["year=2026"] * 4)                     # one attempt and three retries
        self.assertEqual(log["errors"], ["year 2026: HTTP 502"])
        self.assertEqual(self.raw_files(), [])

    def test_a_block_stops_the_run_without_retrying(self):
        self.seed()
        self.api.fail["year=2026"] = 403
        code, log = self.run_main()
        self.assertEqual((code, log["status"], log["blocked"]), (1, "failed", True))
        self.assertEqual(self.api.calls, ["year=2026"])

    def test_an_earlier_year_that_fails_is_asked_for_again_by_the_next_run(self):
        self.seed()
        self.api.fail["year=2025"] = 502
        code, log = self.run_main()
        self.assertEqual((code, log["status"]), (0, "partial"))
        self.assertEqual(self.api.calls[:2], ["year=2026", "year=2025"])
        self.assertNotIn("year=2024", self.api.calls)                           # the walk stops at the failure
        self.assertEqual(sorted(log["years"]), ["2026"])
        del self.api.fail["year=2025"]
        code, log = self.run_main(datetime(2026, 11, 27, 21, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"]), (0, "ok"))
        self.assertEqual(self.api.calls, ["year=2026", "year=2025", "year=2024", "year=2023", "year=2022"])
        self.assertEqual(sorted(log["years"]), ["2025", "2026"])

    def test_a_failing_date_query_stops_the_run_and_the_date_is_asked_for_again(self):
        self.seed()
        self.run_main()
        self.api.put("2026-11-28")
        self.api.fail["date=2026-11-28"] = 502
        code, log = self.run_main(datetime(2026, 11, 28, 9, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"], log["new_raw_files"]), (0, "partial", []))
        del self.api.fail["date=2026-11-28"]
        code, log = self.run_main(datetime(2026, 11, 28, 21, 0, tzinfo=timezone.utc))
        self.assertEqual((code, log["status"], len(log["new_raw_files"])), (0, "ok", 1))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import csv
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


cndc = load_module(
    "bolivia_cndc_datos_hidrologicos_scraper",
    "scrapers/bolivia/cndc_datos_hidrologicos/bolivia_cndc_datos_hidrologicos_scraper.py",
)

DATES = ["2026-10-13", "2026-10-14"]


def response(day: str, corani_end: float = 52.50182, available: list[str] | None = None) -> bytes:
    """A trimmed response with the same structure as the real endpoint."""
    return json.dumps({
        "fecha": day,
        "fechas_disponibles": list(reversed(available or DATES)),
        "sistemas": [
            {"sistema": "CORANI", "embalses": [
                {"embalse": "CORANI", "caudal_m3s": 2.3, "capacidad_hm3": 137.996,
                 "vol_inicial_hm3": 53.23, "vol_final_hm3": corani_end},
                {"embalse": "SAN JOSE 2", "caudal_m3s": None, "capacidad_hm3": None,
                 "vol_inicial_hm3": 0.0175, "vol_final_hm3": None}]},
            {"sistema": "MISICUNI", "embalses": [
                {"embalse": "MISICUNI", "caudal_m3s": 0.61, "capacidad_hm3": 147.0832,
                 "vol_inicial_hm3": 111.28, "vol_final_hm3": 111.08504}]},
        ],
    }).encode("utf-8")


class ParseTests(unittest.TestCase):
    def test_rows_per_reservoir(self):
        rows = cndc.parse(json.loads(response("2026-10-14")))
        self.assertEqual([(r["system"], r["reservoir"]) for r in rows],
                         [("CORANI", "CORANI"), ("CORANI", "SAN JOSE 2"), ("MISICUNI", "MISICUNI")])
        self.assertEqual(rows[0]["vol_final_hm3"], 52.50182)
        self.assertEqual(rows[1]["capacidad_hm3"], "")
        self.assertEqual({r["record_date"] for r in rows}, {"2026-10-14"})

    def test_rolling_date_list_does_not_change_data_hash(self):
        a = json.loads(response("2026-10-14", available=DATES))
        b = json.loads(response("2026-10-14", available=DATES + ["2026-10-15"]))
        self.assertEqual(cndc.data_hash(a), cndc.data_hash(b))
        c = json.loads(response("2026-10-14", corani_end=52.6))
        self.assertNotEqual(cndc.data_hash(a), cndc.data_hash(c))


class CaptureTests(unittest.TestCase):
    def run_once(self, out: Path, bodies: dict[str, bytes], index: bytes | None = None,
                 status: int = 200) -> dict:
        def fake_fetch(url):
            if "?fecha=" not in url:
                return (200, index or response(DATES[-1]), "")
            day = url.split("?fecha=")[1]
            if status != 200:
                return (status, b"", f"HTTP {status}")
            return (200, bodies.get(day, json.dumps({"sin_datos": True, "fecha": day}).encode()), "")

        with mock.patch.object(cndc, "RAW_DIR", out / "raw"), \
                mock.patch.object(cndc, "TS_DIR", out / "timeseries"), \
                mock.patch.object(cndc, "RUN_LOG_DIR", out / "run_logs"), \
                mock.patch.object(cndc, "PAUSE_SECONDS", 0), \
                mock.patch.object(cndc, "fetch", side_effect=fake_fetch):
            summary = {"http_200": 0, "new_raw_files": [], "sin_datos": [], "date_mismatches": [], "errors": [],
                       "blocked": False}
            cndc.capture(summary)
            cndc.rebuild(summary)
            return summary

    def test_rerun_with_new_date_list_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            first = self.run_once(out, {d: response(d) for d in DATES})
            later = {d: response(d, available=DATES + ["2026-10-15"]) for d in DATES}
            second = self.run_once(out, later)
            self.assertEqual(len(first["new_raw_files"]), 2)
            self.assertEqual(second["new_raw_files"], [])
            rows = list(csv.DictReader((out / "timeseries/bolivia_cndc_datos_hidrologicos.csv").open()))
            self.assertEqual(len(rows), 6)

    def test_changed_data_is_kept_as_a_new_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self.run_once(out, {d: response(d) for d in DATES})
            summary = self.run_once(out, {"2026-10-13": response("2026-10-13"),
                                          "2026-10-14": response("2026-10-14", corani_end=52.6)})
            self.assertEqual(len(summary["new_raw_files"]), 1)
            self.assertEqual(summary["dates_with_revisions"], ["20261014"])
            latest = list(csv.DictReader((out / "timeseries/bolivia_cndc_datos_hidrologicos.csv").open()))
            corani = [r for r in latest if r["record_date"] == "2026-10-14" and r["reservoir"] == "CORANI"]
            self.assertEqual(corani[0]["vol_final_hm3"], "52.6")
            self.assertEqual(corani[0]["n_versions"], "2")
            all_rows = list(csv.DictReader(
                (out / "timeseries/bolivia_cndc_datos_hidrologicos_all_versions.csv").open()))
            self.assertEqual(len(all_rows), 9)

    def test_sin_datos_is_not_stored(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            summary = self.run_once(out, {"2026-10-14": response("2026-10-14")})
            self.assertEqual(summary["sin_datos"], ["2026-10-13"])
            self.assertEqual(len(list((out / "raw").rglob("*.json"))), 1)

    def test_block_stops_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            summary = self.run_once(out, {}, status=429)
            self.assertTrue(summary["blocked"])
            self.assertEqual(len(summary["fetch_log"]), 1)


class FetchTests(unittest.TestCase):
    def test_block_is_not_retried(self):
        response_429 = mock.Mock(status_code=429, content=b"")
        with mock.patch.object(cndc.requests, "get", return_value=response_429) as get:
            status, body, err = cndc.fetch("https://example.invalid/x")
        self.assertEqual((status, body), (429, b""))
        self.assertEqual(get.call_count, 1)


if __name__ == "__main__":
    unittest.main()

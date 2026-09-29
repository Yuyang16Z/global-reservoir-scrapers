from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from datetime import date
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


cammesa = load_module(
    "argentina_cammesa_hidro_scraper",
    "scrapers/argentina/cammesa_hidro/argentina_cammesa_hidro_scraper.py",
)


def page(level: str = "415.22", token: str = "AAAA", day: str = "09/25/2026") -> bytes:
    """A trimmed page with the same structure as the real snapshot (latin-1)."""
    return f"""<html><head><title>Datos de Embalses y Centrales Hidráulicas - {day}</title></head><body>
<h3 style="position:absolute;left:0;top:15;width:958;">
     SITUACION DE CUENCAS HIDRAULICAS DEL {day}</h3>
<table style="position:absolute;left:520;top:140;">
  <tr><td colspan="3">PLANICIE BANDERITA</td></tr><tr><td colspan="3">Barreales</td></tr>
  <tr xmlns=""><td align="right">Cota Hoy:</td><td>{level} msnm</td></tr>
  <tr xmlns=""><td align="right">Cota Max:</td><td>422.50 msnm</td></tr>
  <tr xmlns=""><td align="right">Cota Min:</td><td>410.50 msnm</td></tr>
</table>
<table style="position:absolute;left:350;top:1350;">
  <tr><td colspan="3">RIO GRANDE</td></tr>
  <tr xmlns=""><td align="right">Cota Hoy:</td><td>874.17 msnm</td></tr>
  <tr xmlns=""><td align="right">Turbinado:</td><td>18 m3/s</td></tr>
  <tr xmlns=""><td align="right">Bombeo:</td><td>393.75 MWh</td></tr>
</table>
<table style="position:absolute;left:830;top:335;">
  <tr><td colspan="3">EL CHAÑAR</td></tr><tr><td>400 m3/s</td></tr>
</table>
<table><tr><td>Niveles a principio de mes ( Manejo de Aguas )</td></tr><tr><td>Enero</td><td>381.00</td></tr></table>
<script id="f5_cspm">(function(){{var f5_cspm={{f5_p:'{token}'}}}})();</script>
</body></html>""".encode("latin-1")


class ParseTests(unittest.TestCase):
    def test_page_date_and_boxes(self):
        page_date, res, riv = cammesa.parse_page(page())
        self.assertEqual(page_date, "2026-09-25")  # printed MM/DD/YYYY
        self.assertEqual([r["box_name"] for r in res], ["PLANICIE BANDERITA", "RIO GRANDE"])
        self.assertEqual(res[0]["box_subname"], "Barreales")
        self.assertEqual(res[0]["level_today_masl"], 415.22)
        self.assertEqual(res[0]["level_min_masl"], 410.5)
        self.assertIsNone(res[0]["turbined_m3s"])
        self.assertEqual(res[1]["pumping_mwh"], 393.75)
        self.assertEqual([(r["box_name"], r["flow_m3s"]) for r in riv], [("EL CHAÑAR", 400.0)])

    def test_cdn_token_does_not_change_content_hash(self):
        self.assertEqual(cammesa.content_hash(page(token="AAAA")), cammesa.content_hash(page(token="ZZZZ")))
        self.assertNotEqual(cammesa.content_hash(page(level="415.22")), cammesa.content_hash(page(level="415.23")))


class CaptureTests(unittest.TestCase):
    def run_capture(self, out: Path, bodies: dict[str, bytes]) -> dict:
        def fake_fetch(url):
            ymd = url.split("/hidro/")[1][:8]
            return (200, bodies[ymd], "") if ymd in bodies else (404, b"", "")

        with mock.patch.object(cammesa, "RAW_DIR", out / "raw"), \
                mock.patch.object(cammesa, "TS_DIR", out / "timeseries"), \
                mock.patch.object(cammesa, "RUN_LOG_DIR", out / "run_logs"), \
                mock.patch.object(cammesa, "PAUSE_SECONDS", 0), \
                mock.patch.object(cammesa, "fetch", side_effect=fake_fetch):
            summary = {"http_200": 0, "http_404": 0, "new_raw_pages": [], "errors": []}
            cammesa.capture(date(2026, 9, 24), date(2026, 9, 25), summary)
            cammesa.rebuild(summary)
            return summary

    def test_rerun_with_new_cdn_token_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            first = self.run_capture(out, {"20260925": page(token="AAAA")})
            second = self.run_capture(out, {"20260925": page(token="BBBB")})
            self.assertEqual(len(first["new_raw_pages"]), 1)
            self.assertEqual(second["new_raw_pages"], [])
            self.assertEqual(second["http_404"], 1)
            rows = list(csv.DictReader((out / "timeseries/argentina_cammesa_hidro_reservoirs.csv").open()))
            self.assertEqual(len(rows), 2)

    def test_changed_data_is_kept_as_a_new_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self.run_capture(out, {"20260925": page(level="415.22")})
            summary = self.run_capture(out, {"20260925": page(level="415.30")})
            self.assertEqual(len(summary["new_raw_pages"]), 1)
            all_rows = list(csv.DictReader(
                (out / "timeseries/argentina_cammesa_hidro_reservoirs_all_versions.csv").open()))
            latest = list(csv.DictReader((out / "timeseries/argentina_cammesa_hidro_reservoirs.csv").open()))
            self.assertEqual(len(all_rows), 4)
            self.assertEqual({r["n_versions"] for r in latest}, {"2"})
            self.assertEqual(len(list((out / "raw").glob("*.html"))), 2)


class FetchTests(unittest.TestCase):
    def test_404_is_not_retried(self):
        response = mock.Mock(status_code=404, content=b"")
        with mock.patch.object(cammesa.requests, "get", return_value=response) as get:
            status, body, err = cammesa.fetch("https://example.invalid/x")
        self.assertEqual((status, body), (404, b""))
        self.assertEqual(get.call_count, 1)


if __name__ == "__main__":
    unittest.main()

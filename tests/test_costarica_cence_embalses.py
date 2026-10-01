from __future__ import annotations

import csv
import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def page(arenal: list[str], year: str = "2026", stamp: str = "1790844176859", hour: str = "1:00",
         banner: str = "02:42") -> bytes:
    """Trimmed copy of the Boletin Intra-Diario structure: one chart and the hourly table."""
    cats = "".join(f"<category label='{d:02d}/01/{year}'/>" for d in range(1, len(arenal) + 1))
    real = "".join(f"<set value='{v}'/>" for v in arenal)
    prog = "".join("<set value='544.1'/>" for _ in arenal)
    xml = (f"<chart caption='Embalse Arenal' subCaption='{year}' yAxisName='Msnm'><categories>{cats}</categories>"
           f"<dataset seriesName='Programado'>{prog}</dataset><dataset seriesName='Real'>{real}</dataset></chart>")
    return f"""<html><body><span class="info-banner">{banner}</span>
<span class="ui-panel-title">Niveles de Embalse jue, 01 oct {year} - Hora: {hour}</span>
<table><tbody><tr data-ri="0" class="ui-widget-content ui-datatable-even" role="row"><td role="gridcell">Arenal</td><td role="gridcell">N/A</td><td role="gridcell">543.04</td></tr></tbody></table>
<script type="text/javascript"> vargraficoEmbalseArenal{stamp} = new FusionCharts('MSLine', 'g', '100%', '500', '0', '1');
vargraficoEmbalseArenal{stamp}.setDataXML("{xml}");
vargraficoEmbalseArenal{stamp}.render('graficoContainerEmbalseArenal'); </script></body></html>""".encode("utf-8")


class CenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["OUTPUT_DIR"] = self.tmp.name
        self.mod = load_module("costarica_cence_embalses_scraper",
                               "scrapers/costarica/cence_embalses/costarica_cence_embalses_scraper.py")

    def tearDown(self):
        os.environ.pop("OUTPUT_DIR", None)
        self.tmp.cleanup()

    def summary(self):
        return {"errors": [], "new_raw_pages": [], "unchanged": 0}

    def daily(self):
        with (Path(self.tmp.name) / "timeseries" / "costarica_cence_embalses_daily.csv").open(encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def test_parse(self):
        parsed = self.mod.parse_page(page(["544.11", "544.15", "null"]))
        days = parsed["charts"]["Arenal"]["days"]
        self.assertEqual(days["2026-01-01"]["level_real_masl"], 544.11)
        self.assertNotIn("level_real_masl", days["2026-01-03"])          # future day: null
        self.assertEqual(days["2026-01-03"]["level_programado_masl"], 544.1)
        self.assertEqual(parsed["table"]["reading_local"], "2026-10-01T01:00")

    def test_request_specific_parts_do_not_create_versions(self):
        caps, s = [], self.summary()
        t0 = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)
        self.mod.store(page(["544.11"], stamp="1", banner="02:42"), t0, "live", caps, s)
        self.mod.store(page(["544.11"], stamp="2", banner="02:43", hour="2:00"), t0.replace(hour=9), "live", caps, s)
        self.assertEqual(len(caps), 1)                                    # same daily data -> one stored page
        self.assertEqual(s["unchanged"], 1)

    def test_revision_and_year_accumulation(self):
        caps, s = [], self.summary()
        self.mod.store(page(["544.11", "544.15"], year="2025"), datetime(2025, 12, 31, 13, tzinfo=timezone.utc),
                       "live", caps, s)
        self.mod.store(page(["544.11", "544.20"], year="2025"), datetime(2025, 12, 31, 21, tzinfo=timezone.utc),
                       "live", caps, s)
        self.mod.store(page(["543.90"], year="2026"), datetime(2026, 1, 1, 13, tzinfo=timezone.utc), "live", caps, s)
        self.mod.rebuild(caps, s)
        rows = {(r["observation_date"]): r for r in self.daily()}
        self.assertEqual(rows["2025-01-02"]["level_real_masl"], "544.2")  # later capture wins
        self.assertEqual(rows["2026-01-01"]["level_real_masl"], "543.9")  # new year kept beside the old one
        self.assertEqual(s["revisions"], 1)


if __name__ == "__main__":
    unittest.main()

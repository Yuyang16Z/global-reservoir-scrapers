from __future__ import annotations

import csv
import gzip
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


def card(name: str, pct: str, mg: str, cap: str, as_of: str) -> str:
    return f"""<div class="res-card"><div class="res-head"><div><h2>{name}</h2>
<div class="loc">Kingston &amp; St. Andrew supply</div></div>
<span class="status-pill watch"><span class="dot"></span>Conserve</span></div>
<div class="gauge-row"><div class="tank" aria-hidden="true"><div class="tank-fill" data-level="{pct}"></div></div>
<div class="gauge-figures"><div class="g-pct">{pct}<span>%</span></div>
<div class="g-mg"><strong>{mg}</strong> of {cap.split(' MG')[0]} MG capacity</div>
<div class="g-trend down">Trending down vs. last reading</div></div></div>
<div class="res-meta"><span>Capacity: <strong> {cap}</strong></span><span>Reading: <strong>{mg} MG</strong></span>
<span>As of: <strong>{as_of}</strong></span></div></div>"""


def page(mona_mg: str = "269.6", as_of: str = "01 Oct 26") -> bytes:
    """Trimmed copy of the 2026 'Dam & Reservoir Levels' layout."""
    return ("<html><body><div class='res-grid'>"
            + card("Mona Reservoir", "33.3", mona_mg, "808.5 MG / 3,675 ML", as_of)
            + card("Hermitage Dam", "40.1", "157.9", "393.5 MG / 1,789 ML", as_of)
            + "</div></body></html>").encode("utf-8")


class NwcTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["OUTPUT_DIR"] = self.tmp.name
        self.mod = load_module("jamaica_nwc_reservoirs_scraper",
                               "scrapers/jamaica/nwc_reservoirs/jamaica_nwc_reservoirs_scraper.py")

    def tearDown(self):
        os.environ.pop("OUTPUT_DIR", None)
        self.tmp.cleanup()

    def summary(self):
        return {"errors": [], "new_raw_pages": [], "unchanged": 0}

    def rows(self):
        path = Path(self.tmp.name) / "timeseries" / "jamaica_nwc_reservoirs.csv"
        with path.open(encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def test_parse(self):
        parsed = self.mod.parse_page(page())
        self.assertEqual(parsed["layout"], "res_card_2026")
        mona = {r["reservoir"]: r for r in parsed["readings"]}["Mona Reservoir"]
        self.assertEqual(mona["observation_date"], "2026-10-01")   # page date "01 Oct 26", not the fetch time
        self.assertEqual(mona["storage_mg"], 269.6)
        self.assertEqual(mona["storage_pct"], 33.3)
        self.assertEqual(mona["capacity_ml"], 3675.0)                # thousands separator
        self.assertEqual(parsed["problems"], [])

    def test_same_reading_stored_once_and_revision_listed(self):
        caps, s = [], self.summary()
        t0 = datetime(2026, 10, 4, 14, 40, tzinfo=timezone.utc)
        self.mod.store(page(), t0, "live", caps, s)
        self.mod.store(page(), t0.replace(hour=22), "live", caps, s)
        self.assertEqual(len(caps), 1)
        self.assertEqual(s["unchanged"], 1)
        self.mod.store(page(mona_mg="270.1"), t0.replace(day=5), "live", caps, s)   # corrected same-date reading
        self.mod.store(page(mona_mg="250.0", as_of="08 Oct 26"), t0.replace(day=9), "live", caps, s)
        self.mod.rebuild(caps, s)
        rows = {(r["reservoir"], r["observation_date"]): r for r in self.rows()}
        self.assertEqual(rows[("Mona Reservoir", "2026-10-01")]["storage_mg"], "270.1")   # later capture wins
        self.assertEqual(rows[("Mona Reservoir", "2026-10-08")]["storage_mg"], "250")
        self.assertEqual(s["revisions"], 1)

    def test_unreadable_page_is_kept(self):
        caps, s = [], self.summary()
        self.mod.store(b"<html><body><p>Under maintenance</p></body></html>",
                       datetime(2026, 10, 4, 14, 40, tzinfo=timezone.utc), "live", caps, s)
        self.assertEqual(caps[0]["layout"], "unparsed")
        self.assertTrue(s["errors"])
        stored = Path(self.tmp.name) / "raw" / caps[0]["raw_file"]
        self.assertIn(b"Under maintenance", gzip.decompress(stored.read_bytes()))

    def test_reading_dated_after_capture_is_not_kept(self):
        caps, s = [], self.summary()
        self.mod.store(page(as_of="01 Oct 27"), datetime(2026, 10, 4, 14, 40, tzinfo=timezone.utc), "live", caps, s)
        self.assertTrue(any("after the capture" in e for e in s["errors"]))
        self.assertEqual(caps[0]["layout"], "unparsed")


if __name__ == "__main__":
    unittest.main()

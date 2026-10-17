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


def page(rows, headers=("Tarih", "Yükseklik", "Kot", "Hacim", "Doluluk Oranı")) -> bytes:
    """Trimmed copy of the KOSKİ 'baraj değerleri' table: thead and tfoot repeat the header."""
    head = "<tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>"
    body = "".join("<tr>" + "".join(f'<td class="databarajtd">{c}</td>' for c in r) + "</tr>" for r in rows)
    return (f'<html><body><table class="other"><tr><td>13/10/2026</td><td>x</td><td>1</td><td>2</td>'
            f'<td>3</td></tr></table><table id="databaraj" class="table"><thead>{head}</thead>'
            f"<tbody>{body}</tbody><tfoot>{head}</tfoot></table></body></html>").encode("utf-8")


CURRENT = [("13/10/2026", "1018", "1249.68", "17100000", "53"),
           ("12/10/2026", "1019", "1249.69", "17130000", "54")]


class KoskiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["OUTPUT_DIR"] = self.tmp.name
        self.mod = load_module("turkey_koski_scraper", "scrapers/turkey/koski/turkey_koski_scraper.py")

    def tearDown(self):
        os.environ.pop("OUTPUT_DIR", None)
        self.tmp.cleanup()

    def summary(self):
        return {"errors": [], "new_raw_pages": [], "unchanged": 0, "pages_ok": 0}

    def rows(self, name="turkey_koski.csv"):
        with (Path(self.tmp.name) / "timeseries" / name).open(encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def test_parse_reads_columns_by_name(self):
        parsed = self.mod.parse_page(page(CURRENT))
        self.assertEqual(parsed["columns"], ["measurement_date", "yukseklik", "kot", "hacim", "doluluk_orani"])
        self.assertEqual(parsed["rows"][0], {"measurement_date": "2026-10-13", "yukseklik": "1018",
                                             "kot": "1249.68", "hacim": "17100000", "doluluk_orani": "53"})
        self.assertEqual(len(parsed["rows"]), 2)              # the other table on the page is not read
        self.assertEqual(parsed["problems"], [])

    def test_parse_2020_layout(self):
        parsed = self.mod.parse_page(page([("23/09/2020", "919", "1248.69", "16000000", "12", "340")],
                                          ("Tarih", "Yükseklik", "Kot", "Hacim", "Gelen CM", "Gelen LT")))
        self.assertEqual(parsed["columns"], ["measurement_date", "yukseklik", "kot", "hacim", "gelen_cm",
                                             "gelen_lt"])
        self.assertNotIn("doluluk_orani", parsed["rows"][0])   # 'Gelen CM' is not read as the fill ratio
        self.assertEqual(parsed["rows"][0]["gelen_cm"], "12")

    def test_page_without_table(self):
        parsed = self.mod.parse_page(b"<html><body><p>Bakim</p></body></html>")
        self.assertEqual(parsed["rows"], [])
        self.assertTrue(parsed["problems"])

    def test_same_table_stored_once_and_revision_listed(self):
        caps, s = [], self.summary()
        t0 = datetime(2026, 10, 14, 5, 34, 56, tzinfo=timezone.utc)
        self.mod.store("altinapa", page(CURRENT), t0, "live", caps, s)
        self.mod.store("altinapa", page(CURRENT), t0.replace(hour=12), "live", caps, s)
        self.assertEqual(len(caps), 1)
        self.assertEqual(s["unchanged"], 1)
        self.mod.store("bagbasi", page(CURRENT), t0, "live", caps, s)          # same table, other dam: stored
        self.assertEqual(len(caps), 2)
        later = [("14/10/2026", "1017", "1249.67", "17080000", "53"),
                 ("13/10/2026", "1018", "1249.70", "17100000", "53")] + CURRENT[1:]
        self.mod.store("altinapa", page(later), t0.replace(day=15), "live", caps, s)
        self.mod.rebuild(caps, s)
        rows = {(r["dam"], r["measurement_date"]): r for r in self.rows()}
        self.assertEqual(rows[("altinapa", "2026-10-13")]["kot"], "1249.70")    # later capture wins
        self.assertEqual(rows[("altinapa", "2026-10-14")]["kot"], "1249.67")
        self.assertEqual(rows[("altinapa", "2026-10-12")]["captured_utc"], "2026-10-15T05:34:56+00:00")
        self.assertEqual(s["revisions"], 1)
        rev = self.rows("turkey_koski_revisions.csv")[0]
        self.assertEqual((rev["field"], rev["previous_value"], rev["new_value"]), ("kot", "1249.68", "1249.70"))

    def test_old_layout_dates_join_the_series(self):
        caps, s = [], self.summary()
        old = page([("23/09/2020", "919", "1248.69", "16000000", "12", "340")],
                   ("Tarih", "Yükseklik", "Kot", "Hacim", "Gelen CM", "Gelen LT"))
        self.mod.store("altinapa", old, datetime(2020, 9, 23, 3, 58, 46, tzinfo=timezone.utc),
                       "internet_archive", caps, s)
        self.mod.store("altinapa", page(CURRENT), datetime(2026, 10, 14, 5, 34, 56, tzinfo=timezone.utc),
                       "live", caps, s)
        self.mod.rebuild(caps, s)
        rows = {r["measurement_date"]: r for r in self.rows()}
        self.assertEqual(rows["2020-09-23"]["gelen_cm"], "12")
        self.assertEqual(rows["2020-09-23"]["doluluk_orani"], "")
        self.assertEqual(rows["2026-10-13"]["gelen_cm"], "")
        self.assertEqual(s["revisions"], 0)

    def test_column_only_older_page_prints_is_kept(self):
        caps, s = [], self.summary()
        old = page([("12/01/2026", "1107", "1250.57", "19400000", "32", "1640")],
                   ("Tarih", "Yükseklik", "Kot", "Hacim", "Gelen CM", "Gelen LT"))
        self.mod.store("altinapa", old, datetime(2026, 1, 18, 2, 8, 49, tzinfo=timezone.utc),
                       "internet_archive", caps, s)
        self.mod.store("altinapa", page(CURRENT + [("12/01/2026", "1107", "1250.57", "19400000", "54")]),
                       datetime(2026, 10, 14, 5, 34, 56, tzinfo=timezone.utc), "live", caps, s)
        self.mod.rebuild(caps, s)
        row = {r["measurement_date"]: r for r in self.rows()}["2026-01-12"]
        self.assertEqual((row["gelen_lt"], row["doluluk_orani"]), ("1640", "54"))   # both pages' columns
        self.assertTrue(row["raw_file"].startswith("2026/altinapa_20261014T053456Z"))  # the latest page
        self.assertEqual(s["revisions"], 0)

    def test_unreadable_page_is_kept(self):
        caps, s = [], self.summary()
        self.mod.store("bozkir", b"<html><body><p>Bakim</p></body></html>",
                       datetime(2026, 10, 14, 5, 34, 56, tzinfo=timezone.utc), "live", caps, s)
        self.assertTrue(caps[0]["raw_file"].startswith("unparsed/bozkir_"))
        self.assertTrue(s["errors"])
        stored = Path(self.tmp.name) / "raw" / caps[0]["raw_file"]
        self.assertIn(b"Bakim", gzip.decompress(stored.read_bytes()))

    def test_row_dated_after_capture_is_not_kept(self):
        caps, s = [], self.summary()
        self.mod.store("afsar", page([("20/10/2026", "1238", "1238.15", "273390000", "75")] + CURRENT),
                       datetime(2026, 10, 14, 5, 34, 56, tzinfo=timezone.utc), "live", caps, s)
        self.assertTrue(any("after the capture" in e for e in s["errors"]))
        self.mod.rebuild(caps, s)
        self.assertNotIn("2026-10-20", {r["measurement_date"] for r in self.rows()})

    def test_import_names(self):
        imp = Path(self.tmp.name) / "import"
        imp.mkdir()
        (imp / "altinapa_20260908T070343.html").write_bytes(page(CURRENT[1:]))
        (imp / "bagbasi_20200920004518.html").write_bytes(page([("19/09/2020", "1157", "1157.54", "120000000",
                                                                  "60")]))
        (imp / "unknown_20260908T070343.html").write_bytes(page(CURRENT))
        caps, s = [], self.summary()
        self.mod.import_pages(imp, "local_capture", caps, s)
        self.assertEqual(sorted((c["dam"], c["captured_utc"]) for c in caps),
                         [("altinapa", "2026-09-08T07:03:43+00:00"), ("bagbasi", "2020-09-20T00:45:18+00:00")])
        self.assertTrue(any("unknown dam" in e for e in s["errors"]))


if __name__ == "__main__":
    unittest.main()

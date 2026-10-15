from __future__ import annotations

import csv
import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def monthly_page(month: str, year: int, rows: dict[str, list[str]],
                 abbreviated: bool = False) -> str:
    """Trimmed copy of the kariba-reservoir-data table: day, two earlier years
    (level, %), then the current year (level, storage, %, turbine, spillage,
    total). `rows` maps the day cell to the current year's six cells."""
    if abbreviated:   # the 2026-09-24 printing: "Sep-24", spacer cells
        names = [f"{month[:3]}-{str(y)[2:]}" for y in (year - 2, year - 1, year)]
        head = "<tr><td>Day</td>" + "<td></td>".join(f"<td>{n}</td>" for n in names) + "</tr>"
        head += "<tr><td></td>" + "<td>Lake</td><td>Percent</td>" * 2 + "<td>Lake</td></tr>"
    else:
        names = [f"{month} {y}" for y in (year - 2, year - 1, year)]
        head = "<tr><th>Day</th>" + "".join(f"<th>{n}</th>" for n in names) + "</tr>"
        head += "<tr>" + "<th>Lake</th><th>Percent</th>" * 2 + "<th>Lake</th></tr>"
    head += ("<tr><td>Level (m)</td><td>Full</td><td>Level (m)</td><td>Full</td>"
             "<td>Level (m)</td><td>x 109 m3</td><td>Full</td>"
             "<td>Discharge (m3/s)</td><td>Spillage (m3/s)</td><td>Outflow (m3/s)</td></tr>")
    body = ""
    for day, current in rows.items():
        cells = [day, "476.47", "6.72%", "477.62", "14.72%", *current]
        body += "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"
    return f"<html><body><table>{head}{body}</table></body></html>"


def level_only(level: str, storage: str, pct: str) -> list[str]:
    return [level, storage, pct, "", "", ""]


class ZraKaribaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["OUTPUT_DIR"] = self.tmp.name
        self.mod = load_module("zambia_zra_kariba_scraper",
                               "scrapers/zambia/zra_kariba/zambia_zra_kariba_scraper.py")
        self.mod.RAW_DIR.mkdir(parents=True)

    def tearDown(self):
        os.environ.pop("OUTPUT_DIR", None)
        self.tmp.cleanup()

    def capture(self, stamp: str, page: str) -> None:
        (self.mod.RAW_DIR / f"reservoir_data_{stamp}.html").write_text(page, encoding="utf-8")

    def replay(self):
        filled, revisions = [], []
        added, _ = self.mod.replay("monthly", "reservoir_data_", self.mod.parse_monthly,
                                   self.mod.TS_DIR / "zambia_zra_kariba_monthly.csv",
                                   self.mod.MONTHLY_COLUMNS, filled, revisions)
        return added, filled, self.mod.write_revisions(revisions)

    def rows(self) -> dict[str, dict]:
        path = self.mod.TS_DIR / "zambia_zra_kariba_monthly.csv"
        with path.open(encoding="utf-8-sig", newline="") as fh:
            return {r["measurement_date"]: r for r in csv.DictReader(fh)}

    def test_later_capture_fills_outflows_and_keeps_first_fetch(self):
        # 2026-09-07 as captured that day (no outflows yet), then on 09-17.
        self.capture("2026-09-07T130024Z", monthly_page("September", 2026, {
            "6": ["481.41", "27.47", "42.40%", "974", "0", "974"],
            "7": level_only("481.39", "27.38", "42.25%")}))
        self.capture("2026-09-17T120132Z", monthly_page("September", 2026, {
            "6": ["481.41", "27.47", "42.40%", "974", "0", "974"],
            "7": ["481.39", "27.38", "42.25%", "1,229", "0", "1,229"],
            "8": level_only("481.38", "27.33", "42.17%")}))
        added, filled, revisions = self.replay()
        row = self.rows()["2026-09-07"]
        self.assertEqual((row["turbine_discharge_m3s"], row["spillage_m3s"],
                          row["total_outflow_m3s"]), ("1229", "0", "1229"))
        self.assertEqual(row["fetched_at"], "2026-09-07T130024Z")
        self.assertEqual(added, 3)
        self.assertEqual({(f["measurement_date"], f["column"], f["capture"]) for f in filled},
                         {("2026-09-07", c, "2026-09-17T120132Z") for c in
                          ("turbine_discharge_m3s", "spillage_m3s", "total_outflow_m3s")})
        self.assertEqual(revisions, 0)
        self.assertFalse(self.mod.REVISIONS_PATH.exists())
        before = (self.mod.TS_DIR / "zambia_zra_kariba_monthly.csv").read_bytes()
        self.assertEqual(self.replay(), (0, [], 0))       # rerun changes nothing
        self.assertEqual((self.mod.TS_DIR / "zambia_zra_kariba_monthly.csv").read_bytes(), before)

    def test_held_value_is_kept_and_a_restatement_is_listed(self):
        self.capture("2026-10-05T151539Z", monthly_page("October", 2026, {
            "4": ["480.85", "24.74", "38.19%", "1,028", "0", "1,028"]}))
        # The later capture restates the turbine figure and prints the
        # percentage coarser; only the first is a revision.
        self.capture("2026-10-15T120345Z", monthly_page("October", 2026, {
            "4": ["480.85", "24.74", "38%", "1,031", "0", "1,028"]}))
        _, filled, revisions = self.replay()
        row = self.rows()["2026-10-04"]
        self.assertEqual((row["turbine_discharge_m3s"], row["usable_storage_pct"]),
                         ("1028", "38.19"))
        self.assertEqual(filled, [])
        self.assertEqual(revisions, 1)
        with self.mod.REVISIONS_PATH.open(encoding="utf-8-sig", newline="") as fh:
            listed = list(csv.DictReader(fh))
        self.assertEqual(listed, [{"table": "monthly", "measurement_date": "2026-10-04",
                                   "column": "turbine_discharge_m3s", "kept_value": "1028",
                                   "printed_value": "1031", "capture": "2026-10-15T120345Z"}])

    def test_abbreviated_year_header_is_read(self):
        # Capture 2026-09-24T120942Z printed "Sep-24 Sep-25 Sep-26" and parsed to nothing.
        rows = self.mod.parse_monthly(monthly_page("September", 2026, {
            "16": ["481.21", "26.5", "40.89%", "1,473", "0", "1,473"]}, abbreviated=True))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["measurement_date"], "2026-09-16")
        self.assertEqual(rows[0]["live_storage_bcm"], "26.5")
        self.assertEqual(rows[0]["total_outflow_m3s"], "1473")

    def test_unlabelled_row_after_day_30_is_day_31_only_in_a_31_day_month(self):
        october = self.mod.parse_monthly(monthly_page("October", 2026, {
            "30": ["480.01", "20.10", "31.00%", "1,200", "0", "1,200"],
            "": ["480.00", "20.05", "30.92%", "1,210", "0", "1,210"]}))
        self.assertEqual([r["measurement_date"] for r in october],
                         ["2026-10-30", "2026-10-31"])
        self.assertEqual(october[1]["turbine_discharge_m3s"], "1210")
        september = self.mod.parse_monthly(monthly_page("September", 2026, {
            "30": ["481.00", "25.40", "39.10%", "1,100", "0", "1,100"],
            "": ["480.99", "25.35", "39.02%", "1,110", "0", "1,110"]}))
        self.assertEqual([r["measurement_date"] for r in september], ["2026-09-30"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import csv
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRAPER = "scrapers/southafrica/dws_weekly/dws_weekly_scraper.py"

STATIONS = [  # (station_id, metadata name, mirror name, region, capacity)
    ("A1R001", "Alfa", "Alfa Dam", "North", 900.0),
    ("A1R002", "Bravo", "Bravo Dam", "North", 100.0),
    ("B1R001", "Charlie", "Charlie Dam", "South", 800.0),
    ("B1R002", "Delta", "Delta Dam", "South", 50.0),
    ("C1R001", "Echo", "Echo Dam", "East", 40.0),
]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def read_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def page(readings: dict[str, tuple]) -> str:
    """A trimmed mirror reservoir page: the Next.js payload, escaped as in a JS string."""
    rows = "".join(
        '{\\"reading_date\\":\\"%s\\",\\"volume_mcm\\":%s,\\"fill_percentage\\":%s}' % (
            day, "null" if vol is None else vol, "null" if pct is None else pct)
        for day, (vol, pct) in sorted(readings.items(), reverse=True))
    return f"<html><script>self.__next_f.push([1,\"[{rows}]\"])</script></html>"


class Resp:
    def __init__(self, text: str = "", status: int = 200, content: bytes | None = None):
        self.text = text
        self.content = content if content is not None else text.encode()
        self.status_code = status
        self.headers = {"Content-Type": "text/html"}

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        return json.loads(self.text)


class FakeMirror:
    """Serves the mirror's country JSON and per-dam pages; dws.gov.za answers 403."""

    def __init__(self, mod, history: dict[str, dict[str, tuple]], latest: date,
                 extra_items: list[dict] | None = None):
        self.mod = mod
        self.history = history            # slug -> {iso date: (volume, pct)}
        self.failing: set[str] = set()
        self.calls: list[str] = []
        items = []
        for station, _, mirror_name, region, capacity in STATIONS:
            slug = mirror_name.lower().replace(" ", "-")
            items.append(self.item(slug, mirror_name, region, capacity))
        items += extra_items or []
        for item in items:
            readings = self.history.get(item["slug"], {})
            if readings:
                day = max(readings)
                vol, pct = readings[day]
                item["latest"] = {"date": day, "fill_percentage": pct, "volume_mcm": vol}
                item["readings_count"] = len(readings)
        self.payload = {"country": "South Africa", "source": "DWS",
                        "generated": latest.isoformat(),
                        "temporal_coverage": f"2026-05-31/{latest.isoformat()}",
                        "reservoirs": items}

    @staticmethod
    def item(slug, name, region, capacity):
        return {"slug": slug, "name": name, "region": region,
                "capacity_total_mcm": capacity, "readings_count": 0, "latest": None}

    def get(self, url, timeout=None):
        self.calls.append(url)
        if "dws.gov.za" in url:
            return Resp("<html>403 Forbidden</html>", status=403)
        if url == self.mod.MIRROR_URL:
            return Resp(json.dumps(self.payload))
        slug = url.rsplit("/", 1)[-1]
        if slug in self.failing:
            return Resp("busy", status=503)
        if slug in self.history:
            return Resp(page(self.history[slug]))
        return Resp("not found", status=404)

    def page_calls(self) -> int:
        return sum(1 for url in self.calls if url.startswith(self.mod.MIRROR_PAGE_ROOT))


class DwsWeeklyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "data" / "southafrica" / "dws_weekly"
        os.environ["OUTPUT_DIR"] = str(self.out)
        for name in ("DWS_TARGET_DATE", "DWS_DRY_RUN"):
            os.environ.pop(name, None)
        self.mod = load_module("southafrica_dws_weekly_scraper", SCRAPER)
        # Five stations stand in for the bulletin's 220; the thresholds scale with them.
        self.mod.MIRROR_MIN_ROWS = 4
        self.mod.DUPLICATE_MIN_COMMON = 4
        self.mod.SENTINEL_REGIONS = 2
        self.mod.MIRROR_HISTORY_WORKERS = 1
        self.mod.META_DIR.mkdir(parents=True)
        with self.mod.META_PATH.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["station_id", "reservoir", "river", "wma", "prov", "wss",
                        "district_mun", "fsc_mcm"])
            for station, name, _, _, capacity in STATIONS:
                w.writerow([station, name, "River", "1", "NW", "", "DC1", capacity])

    def tearDown(self):
        os.environ.pop("OUTPUT_DIR", None)
        os.environ.pop("DWS_TARGET_DATE", None)
        os.environ.pop("DWS_DRY_RUN", None)
        self.tmp.cleanup()

    # -- helpers ---------------------------------------------------------------
    def archive(self, day: date, pct: dict[str, float], official: bool = False):
        self.mod.TS_DIR.mkdir(parents=True, exist_ok=True)
        path = self.mod.TS_DIR / f"southafrica_dws_weekly_{day:%Y%m%d}.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.mod.TS_COLUMNS)
            w.writeheader()
            for station, value in pct.items():
                w.writerow({"date": f"{day:%Y%m%d}", "station_id": station,
                            "pct_full": f"{value:g}", "water_mcm": "1",
                            "pct_last_year": "50" if official else ""})
        return path

    @staticmethod
    def slug(station: str) -> str:
        mirror_name = next(s[2] for s in STATIONS if s[0] == station)
        return mirror_name.lower().replace(" ", "-")

    def history(self, readings: dict[date, dict[str, float]]) -> dict[str, dict[str, tuple]]:
        out: dict[str, dict[str, tuple]] = {}
        for day, per_station in readings.items():
            for station, pct in per_station.items():
                out.setdefault(self.slug(station), {})[day.isoformat()] = (round(pct / 10, 1), pct)
        return out

    def run_main(self, mirror: FakeMirror) -> tuple[int, dict]:
        with mock.patch.object(self.mod, "make_session", return_value=mirror):
            code = self.mod.main()
        logs = sorted(self.mod.LOG_DIR.glob("*_summary.json"))
        return code, json.loads(logs[-1].read_text(encoding="utf-8")) if logs else {}

    def decisions(self) -> dict[str, dict]:
        return self.mod.load_decisions()

    # -- tests -----------------------------------------------------------------
    def test_page_readings_prefer_payload_and_fill_from_table(self):
        html = page({"2026-09-21": (4986.9, 101.7), "2026-08-31": (None, 99.9)})
        html += ("<table><tbody><tr><td>21 Sep 2026</td><td>101.7%</td><td>4,987</td></tr>"
                 "<tr><td>14 Sep 2026</td><td>100.1%</td><td>4,910</td></tr></tbody></table>")
        readings = self.mod._page_readings(html)
        self.assertEqual(readings["2026-09-21"], {"volume_mcm": 4986.9, "fill_percentage": 101.7})
        self.assertEqual(readings["2026-08-31"], {"volume_mcm": None, "fill_percentage": 99.9})
        self.assertEqual(readings["2026-09-14"], {"volume_mcm": 4910.0, "fill_percentage": 100.1})

    def test_repeated_bulletin_needs_a_near_copy_inside_the_window(self):
        base = {s[0]: 50.0 + i for i, s in enumerate(STATIONS)}
        archived = {date(2026, 6, 8): [{"station_id": k, "pct_full": f"{v:g}"}
                                       for k, v in base.items()]}
        copy = [{"station_id": k, "pct_full": f"{v:g}"} for k, v in base.items()]
        found = self.mod.repeated_bulletin(copy, date(2026, 6, 16), archived)
        self.assertEqual(found["bulletin_date"], "20260608")
        self.assertEqual(found["share"], 1.0)
        changed = [dict(r, pct_full=f"{float(r['pct_full']) + 1:g}") for r in copy[:3]] + copy[3:]
        self.assertIsNone(self.mod.repeated_bulletin(changed, date(2026, 6, 16), archived))
        self.assertIsNone(self.mod.repeated_bulletin(copy, date(2026, 7, 6), archived))

    def test_duplicate_entry_that_reproduces_official_bulletins_is_used(self):
        today = datetime.now(tz=timezone.utc).date()
        latest = today - timedelta(days=1)
        official_days = [latest - timedelta(days=21), latest - timedelta(days=14)]
        values = {s[0]: 60.0 + i for i, s in enumerate(STATIONS)}
        for i, day in enumerate(official_days):
            self.archive(day, {k: v + 10 + i for k, v in values.items()}, official=True)
        history = self.history({latest: values})
        # The true Alfa series shows each official bulletin a few days late, as
        # the mirror did in June 2026; the stray entry repeats one value.
        history["alfa-dam"].update({(day + timedelta(days=2)).isoformat():
                                    (6.0, values["A1R001"] + 10 + i)
                                    for i, day in enumerate(official_days)})
        stray = {day: (2.0, 54.9) for day in
                 [latest.isoformat()] + [(d + timedelta(days=2)).isoformat() for d in official_days]}
        history["alfa-stray"] = stray
        mirror = FakeMirror(self.mod, history, latest, extra_items=[
            FakeMirror.item("alfa-stray", "Alfa", "North", 900.0)])
        # the stray entry comes first in the mirror's order, as Elands Drift Dam does
        mirror.payload["reservoirs"].insert(0, mirror.payload["reservoirs"].pop())
        code, summary = self.run_main(mirror)
        self.assertEqual(code, 0)
        self.assertEqual(summary["status"], "ok")
        rows = read_rows(self.mod.TS_DIR /
                                    f"southafrica_dws_weekly_{latest:%Y%m%d}.csv")
        alfa = next(r for r in rows if r["station_id"] == "A1R001")
        self.assertEqual(alfa["pct_full"], f"{values['A1R001']:g}")
        entries = summary["mirror_diagnostics"]["duplicate_entries"][0]
        self.assertEqual(entries["chosen_slug"], "alfa-dam")
        self.assertEqual({e["slug"]: e["official_bulletins_reproduced"] for e in entries["entries"]},
                         {"alfa-stray": 0, "alfa-dam": 2})

    def test_duplicate_without_official_evidence_keeps_first_entry(self):
        latest = datetime.now(tz=timezone.utc).date() - timedelta(days=1)
        values = {s[0]: 70.0 for s in STATIONS}
        history = self.history({latest: values})
        history["alfa-stray"] = {latest.isoformat(): (2.0, 54.9)}
        mirror = FakeMirror(self.mod, history, latest, extra_items=[
            FakeMirror.item("alfa-stray", "Alfa", "North", 900.0)])
        mirror.payload["reservoirs"].insert(0, mirror.payload["reservoirs"].pop())
        _, summary = self.run_main(mirror)
        rows = read_rows(self.mod.TS_DIR /
                                    f"southafrica_dws_weekly_{latest:%Y%m%d}.csv")
        self.assertEqual(next(r for r in rows if r["station_id"] == "A1R001")["pct_full"], "54.9")
        self.assertIsNone(summary["mirror_diagnostics"]["duplicate_entries"][0]["chosen_slug"])

    def test_catch_up_backfills_missed_week_and_rejects_copies_once(self):
        today = datetime.now(tz=timezone.utc).date()
        d0, d1, d2, d3, latest = (today - timedelta(days=n) for n in (29, 22, 15, 14, 1))
        v0 = {s[0]: 40.0 + i for i, s in enumerate(STATIONS)}
        v1 = {k: v + 3 for k, v in v0.items()}
        v2 = {k: v + 6 for k, v in v0.items()}
        v_latest = {k: v + 9 for k, v in v0.items()}
        self.archive(d0, v0, official=True)
        self.archive(d2, v2)
        history = self.history({d0: v0, d1: v1, d2: v2, d3: v2, latest: v_latest})
        history["echo-dam"].pop(d1.isoformat())          # one dam missing that week
        history["alfa-dam"][(today - timedelta(days=8)).isoformat()] = (1.0, 99.0)  # lone reading
        mirror = FakeMirror(self.mod, history, latest)
        code, summary = self.run_main(mirror)
        self.assertEqual(code, 0)
        self.assertEqual(summary["status"], "ok")             # the newest bulletin
        decisions = {d["reading_date"]: d["decision"] for d in summary["history_catch_up"]["decisions"]}
        self.assertEqual(decisions, {
            d1.isoformat(): "backfilled",
            d3.isoformat(): "rejected_repeats_bulletin",
            (today - timedelta(days=8)).isoformat(): "rejected_too_few_dams",
        })
        backfilled = self.mod.TS_DIR / f"southafrica_dws_weekly_{d1:%Y%m%d}.csv"
        rows = read_rows(backfilled)
        self.assertEqual(len(rows), 4)
        self.assertEqual({r["pct_last_year"] for r in rows}, {""})
        self.assertFalse((self.mod.TS_DIR / f"southafrica_dws_weekly_{d3:%Y%m%d}.csv").exists())
        evidence = read_rows(self.mod.RAW_DIR /
                                        f"reservoirs_earth_history_{d1:%Y%m%d}.csv")
        self.assertEqual(len(evidence), 4)
        self.assertTrue(all(len(r["page_sha256"]) == 64 for r in evidence))
        ledger = self.decisions()
        self.assertEqual(ledger[d3.isoformat()]["repeats_bulletin"], f"{d2:%Y%m%d}")
        self.assertEqual(ledger[d1.isoformat()]["decision"], "backfilled")

        # Second run: every history date is archived or decided, so only the
        # sentinel pages are read and nothing new is written.
        mirror.calls.clear()
        files = sorted(p.name for p in self.out.rglob("*") if p.is_file() and "run_logs" not in p.parts)
        code, summary = self.run_main(mirror)
        self.assertEqual(summary["status"], "already-have")
        self.assertEqual(summary["history_catch_up"]["candidates"], [])
        self.assertEqual(mirror.page_calls(), self.mod.SENTINEL_REGIONS)
        after = sorted(p.name for p in self.out.rglob("*") if p.is_file() and "run_logs" not in p.parts)
        self.assertEqual(files, after)

    def test_catch_up_decides_nothing_when_a_page_fails(self):
        today = datetime.now(tz=timezone.utc).date()
        d1, latest = today - timedelta(days=8), today - timedelta(days=1)
        v1 = {s[0]: 30.0 + i for i, s in enumerate(STATIONS)}
        mirror = FakeMirror(self.mod, self.history({d1: v1, latest: {k: v + 2 for k, v in v1.items()}}),
                            latest)
        mirror.failing.add("delta-dam")
        _, summary = self.run_main(mirror)
        report = summary["history_catch_up"]
        self.assertEqual(report["status"], "incomplete_history_pass")
        self.assertEqual(report["decisions"], [])
        self.assertFalse((self.mod.TS_DIR / f"southafrica_dws_weekly_{d1:%Y%m%d}.csv").exists())
        self.assertEqual(self.decisions(), {})

    def test_targeted_run_keeps_history_evidence_and_refuses_a_copy(self):
        d0, d1 = date(2026, 9, 14), date(2026, 9, 21)
        v0 = {s[0]: 80.0 + i for i, s in enumerate(STATIONS)}
        self.archive(d0, v0)
        mirror = FakeMirror(self.mod, self.history({d0: v0, d1: {k: v - 1 for k, v in v0.items()},
                                                    date(2026, 9, 22): v0,
                                                    date(2026, 9, 28): {k: v + 2 for k, v in v0.items()}}),
                            date(2026, 9, 28))
        os.environ["DWS_TARGET_DATE"] = d1.isoformat()
        code, summary = self.run_main(mirror)
        self.assertEqual((code, summary["status"]), (0, "ok"))
        self.assertTrue(summary["mirror_diagnostics"]["targeted_history_backfill"])
        self.assertTrue((self.mod.RAW_DIR / "reservoirs_earth_history_20260921.csv").exists())
        self.assertFalse((self.mod.RAW_DIR / "reservoirs_earth_south_africa_20260921.json").exists())
        self.assertEqual(self.decisions()["2026-09-21"]["decision"], "backfilled")
        self.assertNotIn("history_catch_up", summary)

        os.environ["DWS_TARGET_DATE"] = "2026-09-22"   # the mirror's re-dated copy of 09-14
        code, summary = self.run_main(mirror)
        self.assertEqual((code, summary["status"]), (0, "repeats_archived_bulletin"))
        self.assertEqual(summary["repeats_bulletin"]["bulletin_date"], "20260914")
        self.assertFalse((self.mod.TS_DIR / "southafrica_dws_weekly_20260922.csv").exists())
        self.assertEqual(self.decisions()["2026-09-22"]["decision"], "rejected_repeats_bulletin")

    def test_dry_run_writes_nothing(self):
        today = datetime.now(tz=timezone.utc).date()
        d1, latest = today - timedelta(days=8), today - timedelta(days=1)
        v1 = {s[0]: 30.0 + i for i, s in enumerate(STATIONS)}
        mirror = FakeMirror(self.mod, self.history({d1: v1, latest: {k: v + 2 for k, v in v1.items()}}),
                            latest)
        os.environ["DWS_DRY_RUN"] = "1"
        before = sorted(str(p) for p in self.out.rglob("*"))
        with mock.patch("sys.stdout"):
            with mock.patch.object(self.mod, "make_session", return_value=mirror):
                self.assertEqual(self.mod.main(), 0)
        self.assertEqual(before, sorted(str(p) for p in self.out.rglob("*")))

    def test_stale_mirror_is_reported_without_catch_up(self):
        stale = datetime.now(tz=timezone.utc).date() - timedelta(days=30)
        mirror = FakeMirror(self.mod, self.history({stale: {s[0]: 50.0 for s in STATIONS}}), stale)
        code, summary = self.run_main(mirror)
        self.assertEqual((code, summary["status"]), (0, "source_unavailable"))
        self.assertIn("stale", summary["mirror_error"])
        self.assertNotIn("history_catch_up", summary)
        self.assertEqual(mirror.page_calls(), 0)


if __name__ == "__main__":
    unittest.main()

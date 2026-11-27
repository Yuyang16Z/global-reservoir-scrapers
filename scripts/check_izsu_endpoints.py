#!/usr/bin/env python3
"""One-off check: does ?date= give the same records as ?year= for the same date?

The İZSU scraper stores a new or changed date from the single-date answer after
finding it in the year answer, and compares the two by hash. Five requests.
"""
import importlib.util
import json
import os
import time
from pathlib import Path

spec = importlib.util.spec_from_file_location("izsu", "scrapers/turkey/izsu/turkey_izsu_scraper.py")
izsu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(izsu)

out = {"checks": []}
status, body, err = izsu.fetch(f"{izsu.ENDPOINT}?year=2026")
days, problem = izsu.read_answer(body) if status == 200 else (None, err)
if days is None:
    out["year_2026"] = problem
else:
    dated = sorted(d for d in days if d)
    out["year_2026"] = {"dates": len(dated), "first": dated[0], "last": dated[-1],
                        "most_facilities_in_a_day": max(len(days[d]) for d in dated),
                        "dates_with_more_than_5": sum(len(days[d]) > 5 for d in dated)}
    picks = [dated[-1], dated[len(dated) // 2], dated[0]]
    for day in picks:
        time.sleep(3)
        status, body, err = izsu.fetch(f"{izsu.ENDPOINT}?date={day}")
        answer, problem = izsu.read_answer(body) if status == 200 else (None, err)
        if answer is None:
            out["checks"].append({"date": day, "problem": problem})
            continue
        got = answer.get(day, [])
        out["checks"].append({
            "date": day, "bytes": len(body), "dates_in_answer": sorted(answer),
            "records": [len(days[day]), len(got)],
            "same_hash": izsu.data_hash(days[day]) == izsu.data_hash(got),
            "same_record_ids": sorted(r.get("id") for r in days[day]) == sorted(r.get("id") for r in got),
            "identical_records": sorted(json.dumps(r, sort_keys=True) for r in days[day])
                                 == sorted(json.dumps(r, sort_keys=True) for r in got)})
time.sleep(3)
status, body, err = izsu.fetch(f"{izsu.ENDPOINT}?date=2015-06-15")
answer, problem = izsu.read_answer(body) if status == 200 else (None, err)
out["date_2015_06_15"] = problem if answer is None else {
    "dates_in_answer": sorted(answer),
    "facilities": sorted(r["damWellFacility"]["name"] for r in answer.get("2015-06-15", []))}

target = Path(os.environ.get("OUTPUT_DIR", "outputs")) / "run_logs"
target.mkdir(parents=True, exist_ok=True)
(target / "check.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print(json.dumps(out, ensure_ascii=False, indent=1))

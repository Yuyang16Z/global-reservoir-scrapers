"""Temporary probe, round 2: what does a fresh ABHT render say, parsed by the
scraper itself, and does Bui's site-data endpoint carry a newer level?

Round 1 showed abht.ma serving a page cached on 2026-10-07 14:15:27 GMT for the
plain URL, while a unique query string got a fresh render with different values
(Hassan 1er 183.3 Mm3 against 183.9 in the cached page).

Runs only on this throwaway branch; prints to the job log.
"""
import importlib.util
import json
import re
import time

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")

spec = importlib.util.spec_from_file_location(
    "abht", "scrapers/morocco/abht/morocco_abht_scraper.py")
abht = importlib.util.module_from_spec(spec)
spec.loader.exec_module(abht)


def parse_abht(label, url):
    r = requests.get(url, headers=abht.HEADERS, timeout=90)
    obs, rows = abht.parse_home(r.content, "probe")
    lm = r.headers.get("last-modified")
    print(f"== ABHT {label}: HTTP {r.status_code} {len(r.content)}B last-modified={lm} "
          f"obs_date={obs} dams={len(rows)}")
    for row in rows:
        print(f"     {row['reservoir_name']:<36} {row['storage_mcm']:>6} Mm3 "
              f"{row['storage_pct']:>5}%  change={row['storage_change_mcm']}")


parse_abht("plain (cached)", abht.HOME_URL)
parse_abht("fresh render #1", f"{abht.HOME_URL}?nocache={time.time_ns()}")
time.sleep(5)
parse_abht("fresh render #2", f"{abht.HOME_URL}?nocache={time.time_ns()}")

r = requests.get("https://buipower.com/api/site-data.php",
                 headers={"User-Agent": UA, "Accept": "application/json"}, timeout=60)
print(f"== Bui site-data.php: HTTP {r.status_code} {len(r.content)}B "
      f"type={r.headers.get('content-type')}")
try:
    j = r.json()
except ValueError:
    print("     not JSON:", r.text[:300].replace("\n", " "))
else:
    print("     top-level keys:", list(j)[:30] if isinstance(j, dict) else type(j).__name__)
    flat = json.dumps(j, ensure_ascii=False)
    print("     dates:", sorted(set(re.findall(r"20\d\d-\d\d-\d\d", flat)))[-12:])
    for m in re.finditer(r'"[^"]*(?:level|reservoir|masl|inflow|outflow)[^"]*"\s*:\s*[^,}\]]{1,60}',
                         flat, flags=re.I):
        print("     ", m.group(0)[:160])

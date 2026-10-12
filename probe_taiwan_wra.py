"""Temporary probe, round 3: is Bui's operations-dashboard.php answered from a
server-side cache, and how does the Operations Centre page call it?

Round 1: the API reports generated_at 2026-09-22 with or without a query string
(x-proxy-cache MISS), and the page references only api/operations-dashboard.php
and api/site-data.php. Its X-Server-Cache header was not printed.

Runs only on this throwaway branch; prints to the job log.
"""
import re
import time

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
H = {"User-Agent": UA, "Accept": "application/json, text/plain, */*",
     "Referer": "https://buipower.com/operations-center"}

for label, url in (("plain", "https://buipower.com/api/operations-dashboard.php"),
                   ("query", f"https://buipower.com/api/operations-dashboard.php?_={time.time_ns()}")):
    r = requests.get(url, headers=H, timeout=60)
    meta = r.json().get("meta", {}) if r.ok else {}
    print(f"== {label}: HTTP {r.status_code} generated_at={meta.get('generated_at')} "
          f"latest={meta.get('latest_date')}")
    for k, v in r.headers.items():
        print(f"     {k}: {v}")

js = requests.get("https://buipower.com/assets/js/operations-live.js",
                  headers={"User-Agent": UA}, timeout=60).text
print(f"== operations-live.js {len(js)} chars")
for m in re.finditer(r"operations-dashboard|site-data|fetch\(|\$\.(?:get|ajax|getJSON)\(|setInterval|cache", js):
    a = max(0, m.start() - 160)
    print("   ...", js[a:m.end() + 220].replace("\n", " ")[:420])

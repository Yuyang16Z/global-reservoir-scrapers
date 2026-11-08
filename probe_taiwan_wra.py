"""Throwaway probe: what does Cape Town's weekly dam-levels page carry now?"""
import re
import time

import requests

H = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")}
PAGE = ("https://www.capetown.gov.za/family-and-home/residential-utility-services/"
        "residential-water-and-sanitation-services/this-weeks-dam-levels/")

t0 = time.time()
r = requests.get(PAGE, headers=H, timeout=60)
print(f"page: HTTP {r.status_code}, {len(r.content)} B, {time.time() - t0:.1f}s, final={r.url}")
p = r.text
tables = re.findall(r"<table\b.*?</table>", p, flags=re.S | re.I)
print("tables:", len(tables))
for t in tables:
    print(re.sub(r"\s+", " ", t)[:2500])
    print("-----")
if not tables:
    for kw in ("Major dams", "Previous week", "Current Dam", "dam levels", "Storage"):
        for m in list(re.finditer(re.escape(kw), p, flags=re.I))[:2]:
            print(f"around {kw!r}:", re.sub(r"\s+", " ", p[max(0, m.start() - 600):m.start() + 1500]))
            print("-----")
print("dates:", sorted(set(re.findall(r"\b\d{1,2}/\d{1,2}/20\d{2}\b|\b\d{1,2} (?:October|November) 2026\b", p)))[:20])
links = sorted(set(re.findall(r'href="([^"]+\.pdf)"', p, re.I)))
print("pdf links:", links)
for link in links:
    if "damlevel" not in link.lower():
        continue
    u = requests.compat.urljoin(PAGE, link)
    h = requests.get(u, headers=H, timeout=60)
    print(f"  {u}: HTTP {h.status_code}, {len(h.content)} B, Last-Modified={h.headers.get('Last-Modified')}, "
          f"type={h.headers.get('Content-Type')}")

"""Temporary probe: can runners read the City of Cape Town's "This week's dam
levels" page, and how is its table laid out?

damlevels.pdf has shown the 2026-10-05 dashboard since 10-06, but the City kept
publishing weekly on this HTML page (latest 26 October 2026). Also check
whether koski.gov.tr answers right now (the 10-26 weekly run timed out).

Runs only on this throwaway branch; prints to the job log.
"""
import html
import re
import time

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
PAGES = {
    "www": "https://www.capetown.gov.za/family-and-home/residential-utility-services/"
           "residential-water-and-sanitation-services/this-weeks-dam-levels/",
    "azure": "https://prd-wa-sa-digitas-coct-cecga8e9crbfajf3.southafricanorth-01.azurewebsites.net/"
             "family-and-home/residential-utility-services/residential-water-and-sanitation-services/"
             "this-weeks-dam-levels/",
}


def get(label, url):
    t0 = time.time()
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=(20, 60))
    except requests.RequestException as exc:
        print(f"== {label}: {type(exc).__name__}: {exc} ({time.time() - t0:.0f}s)")
        return None
    print(f"== {label}: HTTP {r.status_code} {len(r.content)}B in {time.time() - t0:.1f}s final={r.url}")
    for k in ("server", "last-modified", "cache-control", "content-type"):
        if k in r.headers:
            print(f"     {k}: {r.headers[k]}")
    return r


def text(fragment):
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", fragment, flags=re.S | re.I)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", t))).strip()


def show_page(r):
    page = r.text
    print("     dates:", sorted(set(re.findall(
        r"\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+20\d\d\b", text(page))))[-12:])
    tables = re.findall(r"<table\b.*?</table>", page, flags=re.S | re.I)
    print(f"     tables: {len(tables)}")
    for i, t in enumerate(tables):
        rows = re.findall(r"<tr\b.*?</tr>", t, flags=re.S | re.I)
        print(f"   -- table {i}: {len(rows)} rows; opening tag: {t[:160]!r}")
        for row in rows[:40]:
            cells = [text(c) for c in re.findall(r"<t[hd]\b.*?</t[hd]>", row, flags=re.S | re.I)]
            print("      |", " | ".join(cells)[:300])
    # headings and text right before the first table: where the week's date lives
    first = page.lower().find("<table")
    if first > 0:
        print("     text before first table:", text(page[max(0, first - 2500):first])[-600:])
    links = sorted(set(re.findall(r"href=[\"']([^\"']*(?:\.pdf|dam)[^\"']*)", page, flags=re.I)))
    print("     links:", links[:25])


if __name__ == "__main__":
    for label, url in PAGES.items():
        r = get(f"Cape Town {label}", url)
        if r is not None and r.ok:
            show_page(r)
    for slug in ("altinapa", "afsar"):
        get(f"KOSKI {slug}", f"https://www.koski.gov.tr/koski/{slug}-baraj-degerleri")

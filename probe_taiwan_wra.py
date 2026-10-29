"""Temporary probe, round 2: read the weekly dashboard PDF that the City of
Cape Town's "This week's dam levels" page now links, with the scraper's own
parser, and look for Internet Archive copies of the weeks we missed (10-12,
10-19).

capetown_scraper_current.py is a verbatim copy of main's
scrapers/southafrica/capetown_wcwss/southafrica_capetown_wcwss_scraper.py.
Runs only on this throwaway branch; prints to the job log.
"""
import importlib.util
import re
import tempfile
from pathlib import Path
from urllib.parse import urljoin

import requests

spec = importlib.util.spec_from_file_location("ct", "capetown_scraper_current.py")
ct = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ct)

PAGE = ("https://www.capetown.gov.za/family-and-home/residential-utility-services/"
        "residential-water-and-sanitation-services/this-weeks-dam-levels/")


def parse_pdf(label, body):
    if not body.startswith(b"%PDF"):
        print(f"   {label}: not a PDF ({body[:60]!r})")
        return
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "d.pdf"
        p.write_bytes(body)
        obs, rows = ct.parse_dashboard(p, "probe")
    print(f"   {label}: {len(body)}B obs_date={obs} rows={len(rows)}")
    for r in rows:
        print("      ", {k: r.get(k) for k in ("measurement_date", "reservoir_id", "storage_pct",
                                                 "storage_ml", "reading_kind")})


page = requests.get(PAGE, headers=ct.HEADERS, timeout=(20, 60))
print(f"== page HTTP {page.status_code} {len(page.content)}B")
links = sorted(set(re.findall(r"href=[\"']([^\"']*damlevels[^\"']*\.pdf)", page.text, flags=re.I)))
print("   pdf links:", links)
for link in links:
    url = urljoin(page.url, link)
    r = requests.get(url, headers=ct.HEADERS, timeout=(20, 120))
    print(f"== {url}: HTTP {r.status_code} type={r.headers.get('content-type')} "
          f"last-modified={r.headers.get('last-modified')}")
    if r.ok:
        parse_pdf("current link", r.content)

cdx = "https://web.archive.org/cdx/search/cdx"
queries = {
    "page": {"url": PAGE.split("://", 1)[1], "from": "20261001"},
    "media pdfs": {"url": "capetown.gov.za/media/", "matchType": "prefix", "from": "20260901",
                   "filter": "original:.*damlevels.*"},
}
for label, q in queries.items():
    try:
        rows = requests.get(cdx, params={**q, "output": "json",
                                         "fl": "timestamp,original,statuscode,digest"},
                            timeout=90).json()
    except Exception as exc:  # noqa: BLE001
        print(f"== CDX {label}: {type(exc).__name__}: {exc}")
        continue
    print(f"== CDX {label}: {max(len(rows) - 1, 0)} captures")
    seen = set()
    for ts, original, status, digest in rows[1:]:
        print(f"   {ts} {status} {digest[:10]} {original}")
        if status != "200" or digest in seen:
            continue
        seen.add(digest)
        snap = requests.get(f"https://web.archive.org/web/{ts}id_/{original}", timeout=120)
        if original.lower().endswith(".pdf"):
            parse_pdf(f"wayback {ts}", snap.content)
        else:
            heads = re.findall(r"<th[^>]*>\s*(\d{2}/\d{2}/\d{4})\s*</th>", snap.text)
            pdfs = sorted(set(re.findall(r"media/[^\"']*damlevels[^\"']*", snap.text)))
            print(f"   wayback {ts} page: weekly table header dates {heads[:4]}, pdf links {pdfs}")

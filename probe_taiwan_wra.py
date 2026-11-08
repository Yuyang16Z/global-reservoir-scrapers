"""Throwaway probe: how far back do opengov.jp detail-page charts reach, and do they hold the archive's gaps?"""
import importlib.util
import requests
from bs4 import BeautifulSoup

spec = importlib.util.spec_from_file_location("og", "scrapers/japan/opengov/japan_opengov_scraper.py")
og = importlib.util.module_from_spec(spec)
spec.loader.exec_module(og)
GAPS = ["2026-04-23", "2026-05-01", "2026-06-01", "2026-06-21", "2026-06-22", "2026-06-25",
        "2026-06-30", "2026-07-01", "2026-07-12", "2026-08-01", "2026-11-01"]
s = requests.Session()
s.headers.update({"User-Agent": og.USER_AGENT})
lst = og.fetch_html(og.LIST_URL, s)
records, last = og.parse_list_page(lst)
print("list last updated:", last, "records:", len(records))
for slug in ("agikawa", "aha"):
    item = next(r for r in records if r.detail_slug.strip("/").endswith(slug)) if any(
        r.detail_slug.strip("/").endswith(slug) for r in records) else records[0]
    soup = BeautifulSoup(og.fetch_html(item.detail_url, s), "html.parser")
    rows = og.parse_long_history_rows(soup, item.reservoir_id, item.reservoir_name)
    dates = [r["date"] for r in rows]
    flows = og.parse_recent_flow_table(soup, item.reservoir_id, item.reservoir_name)
    print(item.detail_url, "updated:", og.extract_last_updated_source(soup))
    print("  chart labels:", len(dates), "first:", dates[:2], "last:", dates[-3:])
    print("  recent table:", len(flows), sorted(flows)[:1], sorted(flows)[-2:])
    have = set(dates)
    print("  gaps present in chart:", {g: (g in have) for g in GAPS})
    for g in GAPS:
        hit = next((r for r in rows if r["date"] == g), None)
        if hit:
            print("   ", g, hit["Storage Rate (%)"], hit["Storage Volume (千m^3)"], hit["Inflow (m^3/s)"])

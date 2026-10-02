"""Temporary probe: what do GitHub runners get from the WRA endpoints?

Run only on the claude/taiwan-wra-probe branch; prints to the job log.
"""
import json
import re
import socket
import time
from urllib.parse import urljoin

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
JSON_HEADERS = {"User-Agent": UA, "Accept": "application/json, text/plain, */*"}
HTML_HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,*/*"}
SHOW_HEADERS = ("Server", "Content-Type", "Location", "Retry-After", "X-Powered-By",
                "X-AspNet-Version", "Via", "X-Cache", "Set-Cookie", "Cache-Control",
                "Content-Length", "Date")
S = requests.Session()


def get(url, headers=None, label="", n=500, redirects=False):
    t0 = time.time()
    print(f"\n### {label} GET {url}", flush=True)
    try:
        r = S.get(url, headers=headers or JSON_HEADERS, timeout=40,
                  allow_redirects=redirects)
    except Exception as e:
        print(f"-> EXC {type(e).__name__}: {e}")
        return None
    print(f"-> {r.status_code} {r.reason} in {time.time() - t0:.1f}s, {len(r.content)} bytes")
    for k in SHOW_HEADERS:
        if k in r.headers:
            print(f"   {k}: {r.headers[k][:200]}")
    if n:
        print("   body:", r.text[:n].replace("\r", " ").replace("\n", " "))
    return r


def section(title):
    print("\n" + "=" * 8 + f" {title} " + "=" * 8, flush=True)


section("runner")
for host in ("fhy.wra.gov.tw", "opendata.wra.gov.tw"):
    try:
        print(host, sorted({a[4][0] for a in socket.getaddrinfo(host, 443)}))
    except Exception as e:
        print(host, "DNS EXC", e)
r = get("https://ipinfo.io/json", label="runner egress", n=400)

section("v1 historical endpoint (what the scraper calls)")
for d in ("2026-10-01", "2026-09-20", "2026-06-01"):
    get(f"https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date={d}", label=f"v1 Daily {d}")
get("https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Daily?date=2026-09-20",
    headers={"User-Agent": "curl/8.5.0", "Accept": "*/*"}, label="v1 Daily, curl UA")
get("https://fhy.wra.gov.tw/WraApi/v1/Reservoir/RealTime", label="v1 RealTime", n=300)
get("https://fhy.wra.gov.tw/WraApi/v1/Reservoir/Station?$top=3", label="v1 Station", n=300)

section("FHY API v2 candidates")
for path in ("Api/v2/Reservoir/Station", "Api/v2/Reservoir/RealTime",
             "Api/v2/Reservoir/Daily?date=2026-09-20", "Api/v2/Reservoir/Daily",
             "Api/v2/Reservoir/Daily/2026-09-20", "Api/v2/Reservoir/History?date=2026-09-20",
             "Api/v2/swagger/docs/v2", "Api/swagger/docs/v2", "Api/v2/swagger/ui/index",
             "Api/swagger/ui/index", "Api/v2/help", "Api/help"):
    get("https://fhy.wra.gov.tw/" + path, label="v2", n=300)

section("FHY web front-end: which APIs does the official site call?")
seen = set()
for page in ("https://fhy.wra.gov.tw/fhyv2/monitor/reservoir", "https://fhy.wra.gov.tw/fhyv2/",
             "https://fhy.wra.gov.tw/"):
    r = get(page, headers=HTML_HEADERS, label="page", n=300, redirects=True)
    if r is None or r.status_code != 200:
        continue
    scripts = re.findall(r'<script[^>]+src="([^"]+)"', r.text)
    print("   scripts:", scripts[:20])
    texts = [r.text]
    for src in scripts[:15]:
        u = urljoin(r.url, src)
        if u in seen:
            continue
        seen.add(u)
        try:
            js = S.get(u, headers=HTML_HEADERS, timeout=40)
            print(f"   fetched {u} -> {js.status_code}, {len(js.content)} bytes")
            texts.append(js.text)
        except Exception as e:
            print(f"   fetched {u} -> EXC {e}")
    found = set()
    for t in texts:
        found.update(re.findall(r'["\'`]((?:https?://[a-z.]*wra\.gov\.tw)?/?(?:[A-Za-z]+/)?(?:Api|WraApi|api)/v?\d?/?[A-Za-z0-9_/\-{}$.]*)', t))
        found.update(m for m in re.findall(r'["\'`]([^"\'`\s]*[Rr]eservoir[^"\'`\s]*)["\'`]', t)
                     if "/" in m and len(m) < 160)
    print("   api-like strings:")
    for f in sorted(found)[:200]:
        print("     ", f)

section("old ASP.NET reservoir pages")
for page in ("https://fhy.wra.gov.tw/ReservoirPage_2011/StorageCapacity.aspx",
             "https://fhy.wra.gov.tw/ReservoirPage_2011/Statistics.aspx"):
    r = get(page, headers=HTML_HEADERS, label="aspx", n=0, redirects=True)
    if r is not None and r.status_code == 200:
        names = re.findall(r'name="([^"]+)"', r.text)
        print("   form fields:", names[:40])

section("opendata.wra.gov.tw: does the daily-operations dataset keep history?")
uuids = {"51023e88-4c76-4dbc-bbb9-470da690d539": "daily ops",
         "2be9044c-6e44-4856-aad5-dd108c2e6679": "water level",
         "708a43b0-24dc-40b7-9ed2-fca6a291e7ae": "basic info"}
for sw in ("https://opendata.wra.gov.tw/openapi/swagger/v1/swagger.json",
           "https://opendata.wra.gov.tw/openapi/api/OpenData/openapiSwagger-generated",
           "https://data.wra.gov.tw/openapi/swagger/v1/swagger.json"):
    r = get(sw, label="swagger", n=200, redirects=True)
    if r is None or r.status_code != 200:
        continue
    try:
        spec = r.json()
    except Exception as e:
        print("   not JSON:", e)
        continue
    paths = spec.get("paths", {})
    print(f"   {len(paths)} paths")
    for p, ops in paths.items():
        if any(u in p for u in uuids) or "{" in p and len(paths) < 40:
            for method, op in ops.items():
                params = [(q.get("name"), q.get("in"), (q.get("description") or "")[:80])
                          for q in op.get("parameters", [])]
                print(f"   {method.upper()} {p}: {op.get('summary', '')[:80]}")
                for q in params:
                    print("      param", q)
    shown = [p for p in paths if any(u in p for u in uuids)]
    if not shown:
        print("   sample paths:", list(paths)[:15])

base = "https://opendata.wra.gov.tw/api/v2/51023e88-4c76-4dbc-bbb9-470da690d539"
for q in ("?format=JSON&sort=_importdate+asc", "?format=JSON&sort=_importdate+desc&size=3",
          "?format=JSON&page=2&size=100", "?format=JSON&date=2026-09-20"):
    r = get(base + q, label="daily ops", n=0)
    if r is not None and r.status_code == 200:
        try:
            rows = r.json()
            dates = sorted({str(x.get("ObservationTime") or x.get("observationtime")
                                or x.get("_importdate") or "")[:10] for x in rows})
            print(f"   rows={len(rows)} keys={list(rows[0])[:20] if rows else []}")
            print(f"   distinct dates: {dates[:10]}{' ...' if len(dates) > 10 else ''}")
        except Exception as e:
            print("   parse EXC", e, r.text[:200])

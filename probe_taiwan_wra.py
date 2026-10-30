"""Throwaway probe: does ABHT publish dam levels anywhere more often than the
homepage widget? Prints dates and modified stamps only."""
import json
import re
import time

import requests

H = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/146.0.0.0 Safari/537.36")}
DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/20\d{2}\b|\b20\d{2}-\d{2}-\d{2}\b")


def get(url, **kw):
    sep = "&" if "?" in url else "?"
    u = f"{url}{sep}nocache={time.time_ns()}"
    t0 = time.time()
    try:
        r = requests.get(u, headers=H, timeout=60, **kw)
    except Exception as exc:
        print(f"  {url}: {type(exc).__name__}: {exc}")
        return None
    print(f"  {url}: HTTP {r.status_code}, {len(r.content)} B, {time.time() - t0:.1f}s, "
          f"Last-Modified={r.headers.get('Last-Modified')}")
    return r


def text_of(html):
    html = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def dates(html):
    found = {}
    for d in DATE_RE.findall(html):
        found[d] = found.get(d, 0) + 1
    return sorted(found.items(), key=lambda kv: -kv[1])[:12]


print("== homepage widget")
r = get("https://abht.ma/")
if r is not None:
    t = text_of(r.text)
    i = t.find("Taux de remplissage")
    print("  widget:", t[i:i + 700] if i >= 0 else "caption not found")

print("== situation-ressources-en-eau page")
r = get("https://abht.ma/bassin/situation-ressources-en-eau/")
if r is not None and r.ok:
    t = text_of(r.text)
    print("  dates:", dates(r.text))
    for kw in ("remplissage", "Mm3", "Mm³", "%"):
        i = t.find(kw)
        if i >= 0:
            print(f"  around {kw!r}:", t[max(0, i - 300):i + 500])
            break
    links = sorted(set(re.findall(r'href="([^"]+\.(?:pdf|xlsx?|png|jpe?g))"', r.text, re.I)))
    print("  file links:", links[-25:])

print("== one dam page")
r = get("https://abht.ma/Barrage/lalla-takerkoust/")
if r is not None and r.ok:
    t = text_of(r.text)
    print("  dates:", dates(r.text))
    for kw in ("remplissage", "Mm3", "Mm³", "Volume"):
        i = t.find(kw)
        if i >= 0:
            print(f"  around {kw!r}:", t[max(0, i - 300):i + 600])
            break

print("== WordPress REST: routes mentioning barrage / situation")
r = get("https://abht.ma/wp-json/")
if r is not None and r.ok:
    try:
        routes = r.json().get("routes", {})
        hits = [k for k in routes if re.search(r"barrage|situation|remplissage|taux", k, re.I)]
        print("  routes:", hits[:30])
        types = [k for k in routes if k.startswith("/wp/v2/") and k.count("/") == 3]
        print("  wp/v2 types:", types[:60])
    except ValueError:
        print("  not JSON")

for path in ("wp/v2/pages?slug=situation-ressources-en-eau&_fields=id,slug,modified_gmt,date_gmt",
             "wp/v2/barrage?per_page=20&_fields=id,slug,modified_gmt,date_gmt",
             "wp/v2/posts?per_page=10&_fields=id,slug,modified_gmt,date_gmt",
             "wp/v2/pages?per_page=10&orderby=modified&_fields=id,slug,modified_gmt"):
    print(f"== REST {path}")
    r = get(f"https://abht.ma/wp-json/{path}")
    if r is not None and r.ok:
        try:
            print("  ", json.dumps(r.json())[:1500])
        except ValueError:
            print("   not JSON:", r.text[:200])

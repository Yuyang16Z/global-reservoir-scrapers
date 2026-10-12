"""Temporary probe: is abht.ma serving a frozen cached homepage, and does the
Bui Operations Centre show anything newer than its API?

ABHT: since 2026-10-07 14:38Z every scheduled fetch of https://abht.ma/ has
returned byte-identical HTML, nonces and the WPML cookie expiry included (that
expiry is render time + 1 day and reads 2026-10-08 14:15:26 GMT). That never
happened in the 125 fetches before, and it started the day the site's plugins
and theme were upgraded. Compare the plain URL with cache-busted variants and
print the cache headers, the render time and the widget date of each.

Bui: operations-dashboard.php still reports generated_at 2026-09-22. Check
which endpoints the Operations Centre page calls and what they report.

Runs only on this throwaway branch; prints to the job log.
"""
import hashlib
import json
import re
import time
from urllib.parse import urljoin

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
CACHE_KEYS = (
    "date", "server", "age", "cache-control", "expires", "last-modified", "etag",
    "vary", "via", "x-cache", "x-cache-status", "x-cache-hits", "x-proxy-cache",
    "cf-cache-status", "x-litespeed-cache", "x-litespeed-cache-control",
    "x-lsadc-cache", "x-qc-cache", "x-qc-pop", "x-varnish", "x-fastcgi-cache",
    "x-nginx-cache", "x-srcache-fetch-status", "x-powered-by", "x-hcdn-cache-status",
    "x-sucuri-cache", "x-kinsta-cache", "x-rocket-nginx-serving-static", "x-cdn",
    "platform", "panel", "content-length", "transfer-encoding",
)


def get(url, **headers):
    try:
        return requests.get(url, headers={"User-Agent": UA, **headers}, timeout=60)
    except requests.RequestException as exc:
        print(f"== {url}: {type(exc).__name__}: {exc}")
        return None


def show_headers(r):
    for k in CACHE_KEYS:
        if k in r.headers:
            print(f"     {k}: {r.headers[k]}")
    other = sorted(k for k in r.headers if k.lower() not in CACHE_KEYS)
    print(f"     other headers: {', '.join(other)}")


def abht(label, url, **headers):
    r = get(url, **headers)
    if r is None:
        return
    t = r.text
    exp = re.search(r'"expires":"([^"]+)"', t)
    i = t.find("Taux de remplissage")
    date = re.search(r"\d{2}/\d{2}/\d{4}", t[i:i + 3000]) if i >= 0 else None
    hassan = re.search(r"Hassan 1er ([\d.]+)Mm", t)
    print(f"== ABHT {label}: HTTP {r.status_code} {len(r.content)}B "
          f"md5={hashlib.md5(r.content).hexdigest()[:10]} "
          f"render_expiry={exp.group(1) if exp else None} "
          f"widget_date={date.group(0) if date else None} "
          f"hassan_mm3={hassan.group(1) if hassan else None} final={r.url}")
    show_headers(r)


def rest(label, url):
    r = get(url)
    if r is None:
        return None
    print(f"== {label}: HTTP {r.status_code} {len(r.content)}B")
    try:
        return r.json()
    except ValueError:
        print("     not JSON:", r.text[:200].replace("\n", " "))
        return None


def probe_abht():
    abht("plain #1", "https://abht.ma/")
    time.sleep(20)
    abht("plain #2 (+20 s)", "https://abht.ma/")
    abht("query buster", f"https://abht.ma/?nocache={int(time.time())}")
    abht("no-cache request headers", "https://abht.ma/",
         **{"Cache-Control": "no-cache", "Pragma": "no-cache"})
    abht("http", "http://abht.ma/")
    abht("www", "https://www.abht.ma/")
    abht("/fr/", "https://abht.ma/fr/")

    r = get("https://abht.ma/Barrage/hassan-1er/")
    if r is not None:
        t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", r.text, flags=re.S)
        t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", t))
        print(f"== ABHT dam page hassan-1er: HTTP {r.status_code} {len(r.content)}B "
              f"md5={hashlib.md5(r.content).hexdigest()[:10]}")
        print("     dates:", sorted(set(re.findall(r"\b\d{2}/\d{2}/\d{4}\b", t)))[-8:])
        for m in re.finditer(r"[^.]{0,80}(Mm[3³]|%)[^.]{0,40}", t):
            print("     ", m.group(0).strip()[:140])
        show_headers(r)

    types = rest("ABHT REST types", "https://abht.ma/wp-json/wp/v2/types")
    if isinstance(types, dict):
        for slug, ty in types.items():
            base = ty.get("rest_base") or slug
            print(f"     type {slug} rest_base={base}")
            if "barrage" in (slug + base).lower():
                rows = rest(f"ABHT REST {base}",
                            f"https://abht.ma/wp-json/wp/v2/{base}?per_page=20"
                            "&orderby=modified&order=desc")
                for p in rows if isinstance(rows, list) else []:
                    extra = {k: p[k] for k in ("acf", "meta") if p.get(k)}
                    print(f"     {p.get('slug')} date={p.get('date')} "
                          f"modified={p.get('modified')} {json.dumps(extra, ensure_ascii=False)[:300]}")
    for base in ("posts", "pages"):
        rows = rest(f"ABHT REST latest {base}",
                    f"https://abht.ma/wp-json/wp/v2/{base}?per_page=5&orderby=modified"
                    "&order=desc&_fields=id,slug,date,modified")
        for p in rows if isinstance(rows, list) else []:
            print(f"     {p.get('slug')} date={p.get('date')} modified={p.get('modified')}")


def bui_meta(label, url):
    r = get(url, Accept="application/json, text/plain, */*")
    if r is None:
        return
    print(f"== Bui {label}: HTTP {r.status_code} {len(r.content)}B")
    show_headers(r)
    try:
        j = r.json()
    except ValueError:
        print("     not JSON:", r.text[:200].replace("\n", " "))
        return
    print("     meta:", json.dumps(j.get("meta"), ensure_ascii=False)[:600])
    rows = j.get("rows") or []
    print(f"     rows={len(rows)} first={rows[0].get('date') if rows else None} "
          f"last={rows[-1].get('date') if rows else None}")


def probe_bui():
    page = get("https://buipower.com/operations-center")
    refs = set()
    if page is not None:
        title = re.search(r"<title>(.*?)</title>", page.text, flags=re.S)
        print(f"== Bui operations-center: HTTP {page.status_code} {len(page.content)}B "
              f"title={title.group(1).strip() if title else None}")
        print("     dates on page:",
              sorted(set(re.findall(r"20\d\d-\d\d-\d\d", page.text)))[-10:])
        scripts = re.findall(r"<script[^>]+src=[\"']([^\"']+)", page.text)
        bodies = [page.text]
        for src in scripts:
            u = urljoin(page.url, src)
            if "buipower.com" in u:
                s = get(u)
                if s is not None and s.ok:
                    bodies.append(s.text)
                    print(f"     script {u} ({len(s.content)}B)")
        for b in bodies:
            refs.update(re.findall(r"[\"'`]([^\"'`\s]*(?:api/|/data/)[^\"'`\s]*)[\"'`]", b))
            refs.update(re.findall(r"[\"'`]([^\"'`\s]+\.(?:php|json|xlsx)(?:\?[^\"'`\s]*)?)[\"'`]", b))
        print("     endpoint references:", sorted(refs)[:40])
    bui_meta("operations-dashboard.php", "https://buipower.com/api/operations-dashboard.php")
    bui_meta("operations-dashboard.php query buster",
             f"https://buipower.com/api/operations-dashboard.php?_={int(time.time())}")


if __name__ == "__main__":
    probe_abht()
    probe_bui()

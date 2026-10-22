"""Temporary probe: why does Kanagawa's API fail, and is Cape Town's weekly
dashboard really two weeks old?

Kanagawa: japan_kanagawa.yml failed at 2026-10-21 18:15Z and again at 10-22
01:13Z with "fetch failed" - three quick attempts, and the scraper swallows the
error. Cape Town: damlevels.pdf at the fallback URL has shown the 2026-10-05
dashboard since 10-06; the primary URL has not served a PDF since August.

Runs only on this throwaway branch; prints to the job log.
"""
import io
import re
import socket
import time
import urllib.request

import pdfplumber
import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HEADER_KEYS = ("server", "date", "last-modified", "etag", "content-type",
               "content-length", "cache-control", "age", "x-cache", "location",
               "retry-after")


def show(label, url, **headers):
    try:
        r = requests.get(url, headers={"User-Agent": UA, **headers}, timeout=60)
    except requests.RequestException as exc:
        print(f"== {label}: {type(exc).__name__}: {exc}")
        return None
    print(f"== {label}: HTTP {r.status_code} {len(r.content)}B final={r.url}")
    for k in HEADER_KEYS:
        if k in r.headers:
            print(f"     {k}: {r.headers[k]}")
    return r


def probe_kanagawa():
    api = "http://kanagawa-dam.jp/api/summary.php"
    referer = "http://kanagawa-dam.jp/web_data/news_mizugame.html"
    for host in ("kanagawa-dam.jp", "www.kanagawa-dam.jp"):
        try:
            ips = sorted({a[4][0] for a in socket.getaddrinfo(host, 80)})
            print(f"== DNS {host}: {ips}")
        except OSError as exc:
            print(f"== DNS {host}: {type(exc).__name__}: {exc}")
    # Exactly what the scraper sends, with the error it swallows.
    req = urllib.request.Request(api, headers={"User-Agent": UA, "Referer": referer})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            body = r.read()
        print(f"== urllib as the scraper: HTTP {r.status} {len(body)}B "
              f"head={body[:200]!r}")
    except Exception as exc:  # noqa: BLE001 - the point is to see it
        print(f"== urllib as the scraper: {type(exc).__name__}: {exc!r}")
        body = getattr(exc, "read", lambda: b"")()
        if body:
            print(f"     body head: {body[:300]!r}")
    r = show("api via requests", api, Referer=referer)
    if r is not None:
        print("     body head:", r.text[:300].replace("\n", " "))
    show("api https", "https://kanagawa-dam.jp/api/summary.php", Referer=referer)
    show("sibling water-storage-level", "http://kanagawa-dam.jp/api/water-storage-level.php",
         Referer=referer)
    r = show("referer page", referer)
    if r is not None:
        print("     title:", re.findall(r"<title>(.*?)</title>", r.text, flags=re.S)[:1])
    r = show("site root", "http://kanagawa-dam.jp/")
    if r is not None:
        print("     title:", re.findall(r"<title>(.*?)</title>", r.text, flags=re.S)[:1])


def pdf_dates(content):
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            text = " ".join((p.extract_text() or "") for p in pdf.pages[:3])
    except Exception as exc:  # noqa: BLE001
        return f"not a readable PDF ({type(exc).__name__})"
    found = re.findall(r"\b\d{1,2}(?:st|nd|rd|th)?\s*[A-Z][a-z]+\.?\s+20\d\d\b", text)
    return sorted(set(found))[:8]


def probe_capetown():
    urls = {
        "primary": "https://resource.capetown.gov.za/documentcentre/Documents/"
                   "City%20research%20reports%20and%20review/damlevels.pdf",
        "fallback": "https://web1.capetown.gov.za/web1/newsandnotices/"
                    "site-media/documents/damlevels.pdf",
    }
    for label, url in urls.items():
        for suffix in ("", f"?_={time.time_ns()}"):
            r = show(f"Cape Town {label}{' + query' if suffix else ''}", url + suffix)
            if r is not None and r.ok:
                print("     dates in PDF:", pdf_dates(r.content))
    page = show("Cape Town release page",
                "https://web1.capetown.gov.za/web1/newsandnotices/Home/Release/dam-levels")
    if page is not None:
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page.text))
        print("     dates on page:", sorted(set(re.findall(
            r"\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+20\d\d\b", text)))[-8:])
        links = sorted(set(re.findall(r"href=[\"']([^\"']*(?:\.pdf|dam)[^\"']*)", page.text,
                                      flags=re.I)))
        print("     links:", links[:20])


if __name__ == "__main__":
    probe_kanagawa()
    probe_capetown()

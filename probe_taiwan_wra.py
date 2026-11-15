"""Throwaway probe: is abht.ma down, or only some URLs?"""
import time
import requests

H = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")}
for url in ("https://abht.ma/", "https://abht.ma/?nocache=1234", "https://abht.ma/?nocache=abc",
            "https://abht.ma/Barrage/lalla-takerkoust/", "https://abht.ma/wp-json/",
            "https://abht.ma/bassin/situation-ressources-en-eau/", "http://abht.ma/"):
    t0 = time.time()
    try:
        r = requests.get(url, headers=H, timeout=60)
        body = r.text
        i = body.find("Taux de remplissage")
        print(f"{url}: HTTP {r.status_code} {len(r.content)}B {time.time()-t0:.1f}s server={r.headers.get('Server')} "
              f"widget={'yes ' + body[i:i+60].replace(chr(10),' ') if i >= 0 else 'no'} | head={body[:200].replace(chr(10),' ')!r}")
    except Exception as exc:
        print(f"{url}: {type(exc).__name__}: {exc}")

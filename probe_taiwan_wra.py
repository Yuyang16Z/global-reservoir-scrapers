"""Throwaway probe: does www.vra.com answer 200 only when Accept / Accept-Language are sent?"""
import time, requests
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
SETS = {
    "ua_only": {"User-Agent": UA},
    "ua_accept": {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
    "ua_lang": {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"},
    "ua_accept_lang": {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                       "Accept-Language": "en-US,en;q=0.9"},
}
for rnd in range(2):
    for name, h in SETS.items():
        for url in ("https://www.vra.com/resources/facts.php",):
            try:
                r = requests.get(url, headers=h, timeout=60)
                i = r.text.find("Lake Level")
                print(f"round{rnd} {name:15s} {url}: HTTP {r.status_code} {len(r.content)}B "
                      f"{r.text[i:i+90].split(chr(10))[0] if i >= 0 else ''}")
            except Exception as exc:
                print(f"round{rnd} {name:15s}: {type(exc).__name__}: {exc}")
            time.sleep(5)

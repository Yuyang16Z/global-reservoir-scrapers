"""Temporary probe: how long do the Ishikawa and Shimane day files stay up?

Both sites publish one JSON file per JST day. The scrapers fetch only today's
file, so the evening hours are lost whenever the late run starts after
midnight. This checks whether yesterday's (and older) files are still served.
Runs only on this throwaway branch; prints to the job log.
"""
import json
from datetime import datetime, timedelta, timezone

import requests

JST = timezone(timedelta(hours=9))
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
SITES = {
    "ishikawa": "https://kasen.pref.ishikawa.lg.jp/dyn/dps/timeline/{d}/{d}_1_dam_60.json",
    "shimane": "https://www.suibou-shimane.jp/dyn/dps/json/{d}/dam60.json",
}


def summarise(name, text):
    i = text.find("{")
    if i < 0:
        return "no JSON object (soft 404?): " + text[:80].replace("\n", " ")
    payload = json.loads(text[i:])
    keys = []
    if name == "ishikawa":
        for sid, node in payload.items():
            if isinstance(node, dict):
                keys += [str(p.get("time", "")) for p in node.get("data60") or [] if isinstance(p, dict)]
    else:
        keys = [k for k in payload if len(k.split("-")) == 5]
    hours = sorted({k.split("-", 3)[-1] for k in keys if len(k.split("-")) == 5})
    days = sorted({"-".join(k.split("-")[:3]) for k in keys if len(k.split("-")) == 5})
    return f"days={days} {len(hours)} slots" + (f" {hours[0]}..{hours[-1]}" if hours else "")


now = datetime.now(JST)
print("now JST:", now.isoformat(timespec="minutes"))
for name, template in SITES.items():
    for back in (0, 1, 2, 3, 7):
        d = (now - timedelta(days=back)).strftime("%Y%m%d")
        url = template.format(d=d)
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=40)
        except Exception as e:
            print(f"{name} {d} (-{back}d): EXC {type(e).__name__}: {e}")
            continue
        kind = r.headers.get("Content-Type", "")
        info = ""
        if r.status_code == 200:
            try:
                info = summarise(name, r.content.decode("utf-8", errors="replace"))
            except ValueError as e:
                info = f"unparsable: {e}"
        print(f"{name} {d} (-{back}d): HTTP {r.status_code} {kind} {len(r.content)} B  {info}")

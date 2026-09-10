#!/usr/bin/env python3
"""Regenerate stations.json — every dam station the kawabou portal lists.

Walks the portal's own hierarchy, using paths read from its Vue bundle:

    file/files/map/pref/prefarea.json               prefecture codes
    file/files/obslist/idx/pref/twn/<prefCd>.json   towns, with damExistFlg
    file/files/obslist/obs/twnlist/<twnCd>.json     stations; obsList.obsDam = dams

About 51 prefecture codes (Hokkaido is split into four) and ~600 towns: expect
10-15 minutes at the pacing below. Run it occasionally, not on every scrape —
stations are added or retired rarely. Aggregate rows (…合計) are kept here and
filtered by the scraper.
"""
import json, time, urllib.request, urllib.error, ssl, sys
from pathlib import Path
B="https://www.river.go.jp/kawabou/file/files"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
CTX=ssl.create_default_context()
def get(path, attempts=3):
    for i in range(attempts):
        try:
            r=urllib.request.Request(f"{B}/{path}",headers={"User-Agent":UA,"Referer":"https://www.river.go.jp/kawabou/pc/tm"})
            with urllib.request.urlopen(r,timeout=45,context=CTX) as f: return json.loads(f.read())
        except urllib.error.HTTPError as e:
            if e.code==404: return None
        except Exception:
            if i<attempts-1: time.sleep(2*(i+1))
    return None

prefs=get("map/pref/prefarea.json")["prefs"]
pcds=sorted({p["prefCd"] for p in prefs})
pname={p["prefCd"]:(p.get("altPrefNm") or p["prefNm"]) for p in prefs}
print(f"prefCds: {len(pcds)}",file=sys.stderr)

dams={}
twn_seen=set()
for n,pc in enumerate(pcds,1):
    idx=get(f"obslist/idx/pref/twn/{pc}.json")
    time.sleep(0.35)
    if not idx: continue
    towns=[t for t in idx.get("twnInfo",[]) if t.get("damExistFlg")]
    print(f"[{n}/{len(pcds)}] {pname.get(pc,pc)} ({pc}): {len(towns)} towns w/ dams",file=sys.stderr)
    for t in towns:
        tc=t["twnCd"]
        if tc in twn_seen: continue
        twn_seen.add(tc)
        tl=get(f"obslist/obs/twnlist/{tc}.json")
        time.sleep(0.35)
        if not tl: continue
        for o in (tl.get("obsList") or {}).get("obsDam") or []:
            fcd=o["obsFcd"]
            dams[fcd]={"obsFcd":fcd,"obsNm":o.get("obsNm"),"ofcCd":o.get("ofcCd"),
                       "dspFlg":o.get("dspFlg"),
                       "prefCd":tl.get("prefCd"),"prefNm":tl.get("prefNm"),
                       "twnCd":tc,"twnNm":tl.get("twnNm")}
json.dump(list(dams.values()),(Path(__file__).resolve().parent/"stations.json").open("w",encoding="utf-8"),ensure_ascii=False,indent=1)
print(f"\nTOTAL DAM STATIONS: {len(dams)}",file=sys.stderr)

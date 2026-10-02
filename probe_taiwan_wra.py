"""Temporary probe, round 3: where is the FHY API v2 reservoir daily endpoint
documented, what parameters does it take, and how is a key obtained?

Run only on the claude/taiwan-wra-probe branch; prints to the job log.
Long token-like strings are redacted before printing.
"""
import json
import re
import time
from html import unescape
from urllib.parse import urljoin

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
H = {"User-Agent": UA, "Accept": "application/json, text/html, */*"}
S = requests.Session()
TOKEN = re.compile(r"[A-Za-z0-9+/_\-]{16,}={0,2}")
KEYISH = re.compile(r"""((?:apikey|api_key|Lq|o)\s*[=:]\s*["'])[^"']{6,}(["'])""")


def redact(s):
    return TOKEN.sub("<redacted>", KEYISH.sub(r"\1<redacted>\2", s))


def get(url, label="", n=0, **kw):
    t0 = time.time()
    print(f"\n### {label} GET {url}", flush=True)
    try:
        r = S.get(url, headers=H, timeout=40, **kw)
    except Exception as e:
        print(f"-> EXC {type(e).__name__}: {e}")
        return None
    print(f"-> {r.status_code} in {time.time() - t0:.1f}s, {len(r.content)} bytes, "
          f"{r.headers.get('Content-Type', '')}")
    if n:
        print("   body:", redact(r.text[:n].replace("\r", " ").replace("\n", " ")))
    return r


def text_and_links(r):
    html = r.text
    links = re.findall(r"""href=["']([^"'#]+)["'][^>]*>(.*?)</a>""", html, re.S)
    body = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S)
    body = unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body))).strip()
    print("   text:", body[:2500])
    for href, label in links[:60]:
        print("   link:", urljoin(r.url, href), "|", re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", label)).strip()[:60])


def section(t):
    print("\n" + "=" * 8 + f" {t} " + "=" * 8, flush=True)


def dump_spec(spec):
    info = spec.get("info", {})
    print("   title:", info.get("title"), "| version:", info.get("version"))
    print("   description:", redact(str(info.get("description", "")))[:1500])
    print("   host/basePath:", spec.get("host"), spec.get("basePath"), spec.get("servers"))
    print("   security defs:", json.dumps(spec.get("securityDefinitions")
                                          or spec.get("components", {}).get("securitySchemes"),
                                          ensure_ascii=False))
    paths = spec.get("paths", {})
    print(f"   {len(paths)} paths: {list(paths)[:40]}")
    for p, ops in paths.items():
        if "eservoir" not in p:
            continue
        for method, op in ops.items():
            if not isinstance(op, dict):
                continue
            print(f"\n   {method.upper()} {p}  summary={op.get('summary')!r}")
            if op.get("description"):
                print("     description:", redact(str(op["description"]))[:500])
            for q in op.get("parameters", []):
                print("     param:", json.dumps({k: q.get(k) for k in
                                                 ("name", "in", "required", "type", "format",
                                                  "description", "default")
                                                 if k in q}, ensure_ascii=False))
            ok = (op.get("responses") or {}).get("200") or {}
            print("     200:", json.dumps(ok, ensure_ascii=False)[:300])
    defs = spec.get("definitions") or spec.get("components", {}).get("schemas") or {}
    for name, d in defs.items():
        if "eservoir" in name or "Daily" in name:
            props = d.get("properties", {})
            print(f"\n   def {name}: " + "; ".join(
                f"{k}:{v.get('type') or v.get('$ref', '')}"
                + (f" ({v['description']})" if v.get("description") else "")
                for k, v in props.items())[:1500])


section("API landing pages")
for u in ("https://fhy.wra.gov.tw/Api/", "https://fhy.wra.gov.tw/OpenApiv3/",
          "https://fhy.wra.gov.tw/OpenApiv3/v2"):
    r = get(u)
    if r is not None and r.status_code == 200 and "html" in r.headers.get("Content-Type", ""):
        text_and_links(r)

section("OpenApiv3 docs")
found = False
for area in ("Reservoir", "Water", "FHY", "Rain", "River", "Flood", "Common", "Basic",
             "Disaster", "Hydrology", "Default"):
    r = get(f"https://fhy.wra.gov.tw/OpenApiv3/API/{area}/v2/docs", label=area)
    if r is not None and r.status_code == 200:
        try:
            dump_spec(r.json())
            found = True
        except Exception as e:
            print("   not JSON:", e, redact(r.text[:200]))
for u in ("https://fhy.wra.gov.tw/OpenApiv3/swagger/docs/v2",
          "https://fhy.wra.gov.tw/OpenApiv3/swagger/docs/v1",
          "https://fhy.wra.gov.tw/OpenApiv3/swagger/ui/index",
          "https://fhy.wra.gov.tw/OpenApiv3/API/v2/docs"):
    r = get(u, n=300)
    if r is not None and r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
        dump_spec(r.json())

section("front-end reservoir chunk: which v2 endpoints and params?")
app = get("https://fhy.wra.gov.tw/fhyv2/js/app.8496b7f3.js")
if app is not None:
    # webpack chunk map, e.g. {123:"abcd1234",...}[e]+".js"
    names = set(re.findall(r'"js/"\s*\+\s*\(?\s*\{([^}]*)\}', app.text))
    chunks = []
    for block in names:
        for cid, h in re.findall(r'(\w+):"([0-9a-f]{8})"', block):
            chunks.append(f"https://fhy.wra.gov.tw/fhyv2/js/{cid}.{h}.js")
    if not chunks:
        for cid, h in re.findall(r'(\d+):"([0-9a-f]{8})"', app.text):
            chunks.append(f"https://fhy.wra.gov.tw/fhyv2/js/{cid}.{h}.js")
    print("   chunk files:", len(chunks))
    seen = set()
    for u in chunks[:80]:
        r = S.get(u, headers=H, timeout=40)
        if r.status_code != 200:
            continue
        for m in re.finditer(r"Reservoir[A-Za-z/]*", r.text):
            ctx = r.text[max(0, m.start() - 200): m.end() + 220]
            key = m.group(0)
            if key in seen:
                continue
            seen.add(key)
            print(f"\n   [{u.rsplit('/', 1)[1]}] {key}: ...{redact(ctx)}...")

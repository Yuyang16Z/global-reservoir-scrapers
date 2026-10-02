"""Temporary probe, round 2: how does FHY API v2 authenticate, and what does
its reservoir Daily endpoint take and return?

Run only on the claude/taiwan-wra-probe branch; prints to the job log.
Long token-like strings are redacted before printing.
"""
import json
import re
import time
from urllib.parse import urljoin

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
H = {"User-Agent": UA, "Accept": "application/json, text/html, */*"}
S = requests.Session()
TOKEN = re.compile(r"[A-Za-z0-9+/_\-]{16,}={0,2}")


def redact(s):
    return TOKEN.sub("<redacted>", s)


def get(url, label="", n=400, **kw):
    t0 = time.time()
    print(f"\n### {label} GET {url}", flush=True)
    try:
        r = S.get(url, headers=kw.pop("headers", H), timeout=40, **kw)
    except Exception as e:
        print(f"-> EXC {type(e).__name__}: {e}")
        return None
    print(f"-> {r.status_code} in {time.time() - t0:.1f}s, {len(r.content)} bytes, "
          f"{r.headers.get('Content-Type', '')}")
    if n:
        print("   body:", redact(r.text[:n].replace("\r", " ").replace("\n", " ")))
    return r


def section(t):
    print("\n" + "=" * 8 + f" {t} " + "=" * 8, flush=True)


section("swagger UI page")
ui = get("https://fhy.wra.gov.tw/Api/swagger/ui/index", n=0)
spec_urls = []
if ui is not None:
    for m in re.findall(r"""(?:url|discoveryUrl|swaggerUrl)\s*[:=]\s*["']([^"']+)["']""", ui.text):
        print("   ui config url:", m)
        spec_urls.append(urljoin(ui.url, m))
    for m in re.findall(r"""<script[^>]+src=["']([^"']+)["']""", ui.text):
        print("   ui script:", m)
        if "swagger-ui" not in m and "lib/" not in m:
            js = get(urljoin(ui.url, m), label="ui js", n=0)
            if js is not None:
                for u in re.findall(r"""["']([^"']*(?:swagger/docs|swagger\.json)[^"']*)["']""", js.text):
                    print("   js spec url:", u)
                    spec_urls.append(urljoin(ui.url, u))
    print("   ui text:", redact(re.sub(r"\s+", " ", ui.text))[:2500])

section("spec")
spec = None
for u in spec_urls + ["https://fhy.wra.gov.tw/Api/swagger/docs/v1",
                      "https://fhy.wra.gov.tw/Api/swagger/docs/v2",
                      "https://fhy.wra.gov.tw/Api/swagger/v1/swagger.json",
                      "https://fhy.wra.gov.tw/Api/swagger/v2/swagger.json",
                      "https://fhy.wra.gov.tw/Api/swagger.json"]:
    r = get(u, label="spec", n=200)
    if r is not None and r.status_code == 200:
        try:
            spec = r.json()
            print("   -> JSON spec found")
            break
        except Exception:
            pass

if spec:
    info = spec.get("info", {})
    print("info.title:", info.get("title"), "| version:", info.get("version"))
    print("info.description:", redact(str(info.get("description", "")))[:3000])
    print("host/basePath/servers:", spec.get("host"), spec.get("basePath"), spec.get("servers"))
    print("securityDefinitions:", json.dumps(spec.get("securityDefinitions")
                                             or spec.get("components", {}).get("securitySchemes"),
                                             ensure_ascii=False))
    print("global security:", spec.get("security"))
    paths = spec.get("paths", {})
    print(f"{len(paths)} paths; reservoir-related:")
    for p, ops in paths.items():
        if "eservoir" not in p:
            continue
        for method, op in ops.items():
            if not isinstance(op, dict):
                continue
            print(f"\n  {method.upper()} {p}  summary={op.get('summary')!r}")
            if op.get("description"):
                print("    description:", redact(str(op["description"]))[:600])
            for q in op.get("parameters", []):
                print("    param:", json.dumps({k: q.get(k) for k in
                                                ("name", "in", "required", "type", "format",
                                                 "description", "default", "enum")
                                                if k in q}, ensure_ascii=False))
            if op.get("security"):
                print("    security:", op["security"])
            ok = (op.get("responses") or {}).get("200") or {}
            sch = ok.get("schema") or ((ok.get("content") or {}).get("application/json") or {}).get("schema")
            print("    200 schema:", json.dumps(sch, ensure_ascii=False)[:300])
    defs = spec.get("definitions") or spec.get("components", {}).get("schemas") or {}
    for name, d in defs.items():
        if "eservoir" in name:
            props = d.get("properties", {})
            print(f"\n  definition {name}: " + ", ".join(
                f"{k}({v.get('type') or v.get('$ref', '')}{': ' + v['description'] if v.get('description') else ''})"
                for k, v in props.items())[:1500])

section("how does the official front-end send the key?")
app = get("https://fhy.wra.gov.tw/fhyv2/js/app.8496b7f3.js", label="app.js", n=0)
vend = get("https://fhy.wra.gov.tw/fhyv2/js/chunk-vendors.4edb8c79.js", label="vendors", n=0)
for name, r in (("app", app), ("vendors", vend)):
    if r is None:
        continue
    for kw in ("Api/v2", "apikey", "ApiKey", "api_key", "x-api-key", "X-API-KEY", "Authorization",
               "headers", "申請", "Key"):
        for m in re.finditer(re.escape(kw), r.text):
            ctx = r.text[max(0, m.start() - 160): m.end() + 160]
            print(f"   [{name}] {kw}: ...{redact(ctx)}...")
            break

section("key application / docs pages")
for u in ("https://fhy.wra.gov.tw/fhyv2/api", "https://fhy.wra.gov.tw/fhyv2/apikey",
          "https://fhy.wra.gov.tw/fhyv2/opendata", "https://fhy.wra.gov.tw/Api/",
          "https://fhy.wra.gov.tw/Api/v2/", "https://fhy.wra.gov.tw/Api/Account/Register"):
    get(u, label="page", n=300)

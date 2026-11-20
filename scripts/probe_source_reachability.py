#!/usr/bin/env python3
"""Probe how a source answers from the machine this runs on.

A scraper that fails on a GitHub-hosted runner often works from a laptop, and
the reverse. This script records, per URL, each layer separately - DNS, TCP
connect, TLS handshake, HTTP - so "the site is down", "the site refuses this
network" and "the page moved" can be told apart from a runner log.

It reads; it never writes into data/. One GET per URL, no retries.

Usage:
  python3 scripts/probe_source_reachability.py URL [URL ...]
  python3 scripts/probe_source_reachability.py --file targets.txt [--json out.json]

In a targets file, lines starting with '#' are comments. A URL may be followed
by options separated by spaces (use \\s for a space inside a pattern):
  grep=REGEX     print up to 12 matches of REGEX in the response text
  links=REGEX    print up to 40 distinct href/src values matching REGEX
  max=N          raise both limits to N
  snippet=N      print the first N characters instead of 600
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import socket
import ssl
import sys
import time
from urllib.parse import urlsplit

import requests

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 45
SNIPPET = 600


def resolve(host: str) -> tuple[list[str], str | None]:
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        return [], f"{type(exc).__name__}: {exc}"
    ips: list[str] = []
    for info in infos:
        ip = info[4][0]
        if ip not in ips:
            ips.append(ip)
    return ips, None


def tcp_connect(ip: str, port: int) -> str:
    start = time.monotonic()
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as sock:
            sock.settimeout(CONNECT_TIMEOUT)
            sock.connect((ip, port))
        return f"open {time.monotonic() - start:.2f}s"
    except OSError as exc:
        return f"{type(exc).__name__} after {time.monotonic() - start:.1f}s: {exc}"


def tls_handshake(ip: str, port: int, sni: str | None, *, verify: bool = True,
                  minimum: ssl.TLSVersion | None = None,
                  maximum: ssl.TLSVersion | None = None) -> str:
    ctx = ssl.create_default_context()
    if not verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if minimum:
        ctx.minimum_version = minimum
    if maximum:
        ctx.maximum_version = maximum
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as raw:
            raw.settimeout(CONNECT_TIMEOUT)
            raw.connect((ip, port))
            with ctx.wrap_socket(raw, server_hostname=sni) as tls:
                cert = tls.getpeercert() or {}
                subject = ",".join(v for rdn in cert.get("subject", ()) for k, v in rdn if k == "commonName")
                issuer = ",".join(v for rdn in cert.get("issuer", ()) for k, v in rdn if k == "organizationName")
                return (f"ok {tls.version()} {tls.cipher()[0]} cn={subject or '?'} "
                        f"issuer={issuer or '?'} notAfter={cert.get('notAfter', '?')}")
    except (OSError, ssl.SSLError) as exc:
        return f"{type(exc).__name__}: {exc}"


def reverse_dns(ip: str) -> str:
    try:
        return socket.gethostbyaddr(ip)[0]
    except OSError:
        return "-"


def pdf_text(body: bytes) -> str:
    """Text of the first three pages, when pdfplumber is installed."""
    try:
        import io

        import pdfplumber
    except ImportError:
        return "(pdfplumber not installed)"
    try:
        with pdfplumber.open(io.BytesIO(body)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages[:3]]
            return f"[{len(pdf.pages)} page(s)] " + re.sub(r"\s+", " ", " ".join(pages))
    except Exception as exc:  # noqa: BLE001 - diagnostic output only
        return f"(unreadable: {type(exc).__name__}: {exc})"


def http_get(url: str, opts: dict | None = None) -> dict:
    opts = opts or {}
    out: dict = {}
    start = time.monotonic()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en,fr;q=0.8,tr;q=0.7"},
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        out["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
        out["seconds"] = round(time.monotonic() - start, 1)
        return out
    body = resp.content
    out.update({
        "status": resp.status_code,
        "final_url": resp.url,
        "redirects": [f"{r.status_code} {r.headers.get('Location', '')}" for r in resp.history],
        "content_type": resp.headers.get("Content-Type", ""),
        "server": resp.headers.get("Server", ""),
        "bytes": len(body),
        "seconds": round(time.monotonic() - start, 1),
    })
    if body[:4] == b"%PDF":
        out["snippet"] = "<PDF> " + pdf_text(body)[:int(opts.get("snippet") or SNIPPET)]
    else:
        text = resp.text if resp.encoding else body.decode("utf-8", "replace")
        if opts.get("grep"):
            found = [re.sub(r"\s+", " ", m.group(0))[:300]
                     for m in re.finditer(opts["grep"], text, re.I | re.S)]
            out["grep"] = found[:int(opts.get("max") or 12)]
            out["grep_total"] = len(found)
        if opts.get("links"):
            seen: list[str] = []
            for m in re.finditer(r"""(?:href|src|action|data-url)\s*=\s*["']([^"']+)["']""", text, re.I):
                link = m.group(1)
                if re.search(opts["links"], link, re.I) and link not in seen:
                    seen.append(link)
            out["links"] = seen[:int(opts.get("max") or 40)]
        if "html" in out["content_type"].lower():
            title = re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S)
            out["title"] = re.sub(r"\s+", " ", title.group(1)).strip()[:150] if title else ""
            text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", text)
            text = re.sub(r"(?s)<[^>]+>", " ", text)
        out["snippet"] = re.sub(r"\s+", " ", text).strip()[:int(opts.get("snippet") or SNIPPET)]
    return out


def probe(target: tuple[str, dict], tls_matrix: bool) -> dict:
    url, opts = target
    parts = urlsplit(url)
    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    result: dict = {"url": url, "host": host}
    ips, dns_error = resolve(host)
    result["ips"] = ips
    if dns_error:
        result["dns_error"] = dns_error
        return result
    first = ips[0]
    result["ptr"] = reverse_dns(first)
    result["tcp"] = {f"{first}:{port}": tcp_connect(first, port)}
    if parts.scheme == "https" and result["tcp"][f"{first}:{port}"].startswith("open"):
        result["tls"] = {"default": tls_handshake(first, port, host)}
        if tls_matrix or not result["tls"]["default"].startswith("ok"):
            result["tls"]["unverified"] = tls_handshake(first, port, host, verify=False)
            result["tls"]["tls1.2_only"] = tls_handshake(
                first, port, host, verify=False,
                minimum=ssl.TLSVersion.TLSv1_2, maximum=ssl.TLSVersion.TLSv1_2)
            result["tls"]["tls1.3_only"] = tls_handshake(
                first, port, host, verify=False, minimum=ssl.TLSVersion.TLSv1_3)
            result["tls"]["no_sni"] = tls_handshake(first, port, None, verify=False)
            other = host[4:] if host.startswith("www.") else f"www.{host}"
            result["tls"][f"sni={other}"] = tls_handshake(first, port, other, verify=False)
    result["http"] = http_get(url, opts)
    return result


def show(result: dict) -> None:
    print(f"\n=== {result['url']}")
    if "dns_error" in result:
        print(f"  dns   : FAILED {result['dns_error']}")
        return
    print(f"  dns   : {', '.join(result['ips'][:4])}  ptr={result.get('ptr', '-')}")
    for key, value in result.get("tcp", {}).items():
        print(f"  tcp   : {key} {value}")
    for key, value in result.get("tls", {}).items():
        print(f"  tls   : [{key}] {value}")
    http = result.get("http", {})
    if "error" in http:
        print(f"  http  : FAILED after {http.get('seconds')}s {http['error']}")
        return
    print(f"  http  : {http.get('status')} {http.get('bytes')}B {http.get('seconds')}s "
          f"type={http.get('content_type')} server={http.get('server')}")
    if http.get("redirects"):
        print(f"  redir : {' -> '.join(http['redirects'])} => {http.get('final_url')}")
    if http.get("title"):
        print(f"  title : {http['title']}")
    print(f"  body  : {http.get('snippet', '')}")
    if "grep" in http:
        print(f"  grep  : {http['grep_total']} match(es)")
        for found in http["grep"]:
            print(f"    | {found}")
    for link in http.get("links", []):
        print(f"    > {link}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("urls", nargs="*")
    parser.add_argument("--file", help="text file with one URL per line; '#' lines are headings")
    parser.add_argument("--json", help="also write the full results to this path")
    parser.add_argument("--tls-matrix", action="store_true",
                        help="try the TLS variants even when the default handshake works")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    lines = list(args.urls)
    if args.file:
        lines += [line.strip() for line in open(args.file, encoding="utf-8")]
    targets: list[tuple[str, dict]] = []
    for line in lines:
        tokens = line.split()
        if not tokens or not tokens[0].startswith(("http://", "https://")):
            continue
        opts = dict(tok.split("=", 1) for tok in tokens[1:] if "=" in tok)
        targets.append((tokens[0], opts))
    if not targets:
        if args.file and not args.urls:
            print(f"no URLs listed in {args.file}; nothing to probe")
            return 0
        parser.error("no http(s) URLs given")
    urls = [url for url, _ in targets]

    print(f"probing {len(urls)} URL(s) from {socket.gethostname()}")
    try:
        print("egress ip:", requests.get("https://api.ipify.org", timeout=15).text.strip())
    except requests.RequestException as exc:
        print("egress ip: unknown", type(exc).__name__)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda t: probe(t, args.tls_matrix), targets))
    for result in results:
        show(result)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(results, handle, ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

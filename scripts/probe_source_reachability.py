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

Lines in a targets file starting with '#' are printed as section headings.
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


def http_get(url: str) -> dict:
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
        out["snippet"] = "<PDF>"
    else:
        text = resp.text if resp.encoding else body.decode("utf-8", "replace")
        if "html" in out["content_type"].lower():
            title = re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S)
            out["title"] = re.sub(r"\s+", " ", title.group(1)).strip()[:150] if title else ""
            text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", text)
            text = re.sub(r"(?s)<[^>]+>", " ", text)
        out["snippet"] = re.sub(r"\s+", " ", text).strip()[:SNIPPET]
    return out


def probe(url: str, tls_matrix: bool) -> dict:
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
    result["http"] = http_get(url)
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("urls", nargs="*")
    parser.add_argument("--file", help="text file with one URL per line; '#' lines are headings")
    parser.add_argument("--json", help="also write the full results to this path")
    parser.add_argument("--tls-matrix", action="store_true",
                        help="try the TLS variants even when the default handshake works")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    urls = list(args.urls)
    if args.file:
        for line in open(args.file, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    urls = [u for u in urls if u.startswith(("http://", "https://"))]
    if not urls:
        parser.error("no http(s) URLs given")

    print(f"probing {len(urls)} URL(s) from {socket.gethostname()}")
    try:
        print("egress ip:", requests.get("https://api.ipify.org", timeout=15).text.strip())
    except requests.RequestException as exc:
        print("egress ip: unknown", type(exc).__name__)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda u: probe(u, args.tls_matrix), urls))
    for result in results:
        show(result)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(results, handle, ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Throwaway probe: NWC Jamaica certificate chain; VRA 403 page."""
import socket, ssl, subprocess, requests

print("== NWC chain (openssl s_client -showcerts)")
out = subprocess.run("echo | openssl s_client -connect www.nwcjamaica.com:443 -servername www.nwcjamaica.com -showcerts 2>&1",
                     shell=True, capture_output=True, text=True, timeout=60).stdout
for line in out.splitlines():
    if any(k in line for k in (" s:", " i:", "Verify return code", "verify error", "NotBefore", "NotAfter", "a:PKEY")):
        print("  ", line.strip())
print("== NWC leaf AIA / dates")
leaf = subprocess.run("echo | openssl s_client -connect www.nwcjamaica.com:443 -servername www.nwcjamaica.com 2>/dev/null | openssl x509 -noout -subject -issuer -dates -ext authorityInfoAccess",
                      shell=True, capture_output=True, text=True, timeout=60).stdout
print(leaf)

H = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")}
print("== VRA")
for url in ("https://vra.com/resources/facts.php", "https://vra.com/", "https://www.vra.com/", "http://vra.com/"):
    try:
        r = requests.get(url, headers=H, timeout=60)
        hdr = {k: v for k, v in r.headers.items() if k.lower() in ("server", "cf-ray", "x-sucuri-id", "x-cache", "content-type", "location", "x-powered-by")}
        print(f"{url}: HTTP {r.status_code} {len(r.content)}B {hdr}")
        print("   body:", r.text[:600].replace("\n", " "))
    except Exception as exc:
        print(f"{url}: {type(exc).__name__}: {exc}")

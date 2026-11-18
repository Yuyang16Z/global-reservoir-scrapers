"""Throwaway probe: fetch the DigiCert G5 intermediate and check that it completes the NWC chain."""
import subprocess, tempfile, os, certifi, requests
URL = "http://cacerts.digicert.com/DigiCertG5TLSRSA4096SHA3842021CA1-1.crt"
der = requests.get(URL, timeout=60).content
d = tempfile.mkdtemp()
open(os.path.join(d, "i.der"), "wb").write(der)
pem = subprocess.run(["openssl", "x509", "-inform", "DER", "-in", os.path.join(d, "i.der"), "-outform", "PEM"],
                     capture_output=True, text=True).stdout
open(os.path.join(d, "i.pem"), "w").write(pem)
print(subprocess.run(["openssl", "x509", "-in", os.path.join(d, "i.pem"), "-noout", "-subject", "-issuer", "-dates",
                      "-fingerprint", "-sha256"], capture_output=True, text=True).stdout)
print("verify against certifi:", subprocess.run(["openssl", "verify", "-CAfile", certifi.where(), os.path.join(d, "i.pem")],
                                                capture_output=True, text=True).stdout.strip())
bundle = os.path.join(d, "bundle.pem")
open(bundle, "w").write(open(certifi.where()).read() + "\n" + pem)
r = requests.get("https://www.nwcjamaica.com/reservoir.php", timeout=60, verify=bundle,
                 headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"})
print("NWC with bundle:", r.status_code, len(r.content), "Hermitage" in r.text)
print("PEM-BEGIN")
print(pem)
print("PEM-END")

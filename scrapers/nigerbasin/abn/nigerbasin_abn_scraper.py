"""Niger Basin Authority monthly hydrological bulletins.

Source:
- https://www.abn.ne/index.php/fr/centre-de-ressources/gestion-eaux-de-surface/
  bulletins-hydrologiques

Why this source exists in the repo:
- Sections 3 and 4 of every bulletin are titled "RESERVOIRS WATER LEVELS" and
  report the Selingue dam in Mali and the Kainji dam in Nigeria: the month's
  maximum and minimum water level with the date each occurred, the volume at
  each, and the end-of-month volume with its filling rate against a stated
  normal capacity. The bulletin states its own provenance - "hydrological
  observation networks of the National Hydrological Services and Dam
  Authorities of nine (9) member countries" - so these are in-situ readings.
- Both countries are otherwise empty in this project: Nigeria was recorded as a
  dead end (NIHSA behind Cloudflare) and Mali had never been reachable.

Retention class: permanent_archive. ABN keeps published bulletins online, so
this is a resilience copy on the owner's half-monthly cadence rather than a
race against overwriting. The agency HAS changed its document layout once
already - the older /images/documents/Bulletins/ paths still resolve - so the
copy is worth keeping.

Known quirks:
- A month with no new bulletin is normal, not a failure.
- The listing page is the only index; file names are inconsistent
  ("Mois de Juillet 2024" vs "BulletinHydroJuillet2025").

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/<name>.pdf
  metadata/nigerbasin_abn_bulletins.csv   (merged inventory, idempotent)
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import html as html_mod
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
_env = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env).expanduser().resolve() if _env else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
META_DIR = OUTPUT_DIR / "metadata"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"

LIST_URL = ("https://www.abn.ne/index.php/fr/centre-de-ressources/"
            "gestion-eaux-de-surface/bulletins-hydrologiques")
SOURCE_AGENCY = "ABN (Autorite du Bassin du Niger / Niger Basin Authority)"
TIMEOUT = 180
ATTEMPTS = 3
BACKOFFS = (5, 20, 60)
HEADERS = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/146.0.0.0 Safari/537.36")}
COLUMNS = ["file_name", "source_url", "bytes", "sha256", "first_seen"]


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def fetch(url: str) -> requests.Response | None:
    last: Exception | None = None
    for attempt, backoff in enumerate(BACKOFFS, 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
            if r.status_code == 200:
                return r
            last = RuntimeError(f"HTTP {r.status_code}")
        except requests.RequestException as exc:  # noqa: PERF203
            last = exc
        if attempt < ATTEMPTS:
            log(f"  attempt {attempt} failed ({last!r}); retrying in {backoff}s")
            time.sleep(backoff)
    log(f"  giving up on {url}: {last!r}")
    return None


def safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", name)[:110]


def listed_bulletins(page: str) -> dict[str, str]:
    """{url: file name} for every bulletin PDF the listing offers."""
    out: dict[str, str] = {}
    for m in re.finditer(r'href=["\']([^"\']*files/15[^"\']*\.pdf)["\']', page, re.I):
        url = html_mod.unescape(m.group(1))
        out[url] = safe(url.rsplit("/", 1)[-1])
    return out


def main() -> int:
    import hashlib
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    stamp = utc_stamp()
    for d in (RAW_DIR, META_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {"source": "nigerbasin/abn", "started_at": stamp,
                                  "url": LIST_URL}

    resp = fetch(LIST_URL)
    if resp is None:
        # An unreachable host is a source outage, not a scraper failure.
        summary.update(status="source_unavailable", finished_at=utc_stamp())
        (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        log("[warn] abn.ne unreachable; recorded source_unavailable, exiting 0")
        return 0

    listed = listed_bulletins(resp.text)
    if not listed:
        # The listing is the only index; if it stops yielding links the page
        # has changed and that must be loud rather than a quiet empty run.
        summary.update(status="no_bulletins_listed", finished_at=utc_stamp())
        (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        log("[error] listing page fetched but no bulletin link parsed")
        return 1
    log(f"[info] {len(listed)} bulletin(s) listed")

    inventory: dict[str, dict] = {}
    inv_path = META_DIR / "nigerbasin_abn_bulletins.csv"
    if inv_path.is_file():
        with inv_path.open(encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                inventory[row["file_name"]] = row

    added = 0
    for url, name in sorted(listed.items(), key=lambda kv: kv[1]):
        path = RAW_DIR / name
        if path.is_file() and path.stat().st_size > 20000 and name in inventory:
            continue
        r = fetch(url)
        if r is None or r.content[:4] != b"%PDF" or len(r.content) < 20000:
            log(f"  [skip] {name}: not a usable PDF")
            continue
        path.write_bytes(r.content)
        inventory[name] = {
            "file_name": name, "source_url": url, "bytes": str(len(r.content)),
            "sha256": hashlib.sha256(r.content).hexdigest(),
            "first_seen": inventory.get(name, {}).get("first_seen", stamp),
        }
        added += 1
        log(f"  [new] {name} ({len(r.content)} bytes)")
        time.sleep(1.5)

    with inv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore",
                           lineterminator="\n")
        w.writeheader()
        for key in sorted(inventory):
            w.writerow(inventory[key])

    summary.update(status="ok", listed=len(listed), added=added,
                   held=len(inventory), finished_at=utc_stamp())
    (RUN_LOG_DIR / f"{stamp}_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    log(f"[done] {added} new bulletin(s); {len(inventory)} held")
    return 0


if __name__ == "__main__":
    sys.exit(main())

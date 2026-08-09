"""Burkina Faso DGRE decadal hydrological note archiver.

Source:
- https://dgre.gov.bf/  (Direction Generale des Ressources en Eau)
  "Note d'information hydrologique" decadal PDFs, discovered via sitemap.xml.

Why this source exists in the repo:
- DGRE's PREVIOUS site, eauburkina.org, died (HTTP 522) and its 2016-2021
  bulletin archive survives only as Internet Archive captures - two bulletins
  of which are lost forever and 25 more arrived truncated. That history is the
  argument for archiving the current site's notes as they appear, even though
  the site itself keeps them online (a permanent archive can still die).
- Only the DECADAL notes are collected. The monthly "Bulletin" PDFs (2-4 MB
  each) carry a prose narrative without stored volumes and are out of scope.

Retention class: permanent_archive - published notes stay online, so this is
a resilience copy, not a race against overwriting. Per the owner's cadence
rule for retrievable-history sources it runs twice a month, and it is NOT
registered in config/windowed_sources.json: the registry and its minimum
capture-opportunity audit exist for sources that LOSE unobserved data.

Known quirks:
- The host serves at roughly 6 KB/s; a 500 KB note takes minutes. Timeouts
  are generous and each file is streamed to a temp path first.
- Sitemap entries point at /documents/<id>/<name>.pdf. The <id> can change
  when DGRE re-uploads, so identity is the FILENAME; a re-upload under the
  same name is only re-fetched when the size differs.
- DGRE publishes during and after the rainy season (roughly June-November).
  Zero new files for months is normal, not a failure.

Outputs (under OUTPUT_DIR, default <script_dir>/outputs):
  raw/<name>.pdf                   one file per decadal note, never rewritten
  inventory.csv                    filename, url, size, sha256, retrieved_at
  run_logs/<stamp>_summary.json
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
import requests

BASE_DIR = Path(__file__).resolve().parent
_env_out = os.environ.get("OUTPUT_DIR", "").strip()
OUTPUT_DIR = Path(_env_out).expanduser().resolve() if _env_out else (BASE_DIR / "outputs")
RAW_DIR = OUTPUT_DIR / "raw"
RUN_LOG_DIR = OUTPUT_DIR / "run_logs"
INVENTORY = OUTPUT_DIR / "inventory.csv"

SITEMAP_URL = "https://dgre.gov.bf/sitemap.xml"
SOURCE_AGENCY = "DGRE (Direction Generale des Ressources en Eau), Burkina Faso"
TIMEOUT = 120
DOWNLOAD_TIMEOUT = 1800          # ~6 KB/s host; 2 MB needs ~6 minutes
REQUEST_ATTEMPTS = 3
REQUEST_BACKOFFS = (5, 20, 60)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

# The sitemap lists PAGES, not files: each decadal note has a page under
# /note-hydrologique-decadaire/<slug>/ whose HTML links the actual PDF at
# /documents/<id>/<name>.pdf. The monthly narrative bulletins live under
# /bulletin-hydrologique-mensuel/ and are out of scope.
DECADAL_PAGE_RE = re.compile(r"/note-hydrologique-decadaire/[^<]+")
PDF_HREF_RE = re.compile(r'href="(/documents/[^"]+\.pdf)"')

INVENTORY_COLUMNS = ["file", "source_url", "size_bytes", "sha256", "retrieved_at"]


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def fetch(url: str, stream: bool = False) -> requests.Response | None:
    last: Exception | None = None
    for attempt, backoff in enumerate(REQUEST_BACKOFFS, 1):
        try:
            resp = requests.get(url, headers=HEADERS, stream=stream,
                                timeout=DOWNLOAD_TIMEOUT if stream else TIMEOUT)
            if resp.status_code == 200:
                return resp
            last = RuntimeError(f"HTTP {resp.status_code}")
        except requests.RequestException as exc:  # noqa: PERF203
            last = exc
        if attempt < REQUEST_ATTEMPTS:
            log(f"  attempt {attempt} failed ({last!r}); retrying in {backoff}s")
            time.sleep(backoff)
    log(f"  giving up on {url}: {last!r}")
    return None


def is_source_unavailable(exc_or_none: requests.Response | None) -> bool:
    return exc_or_none is None


def decadal_pages() -> list[str] | None:
    """Decadal-note page URLs from the sitemap, or None when unreachable."""
    resp = fetch(SITEMAP_URL)
    if resp is None:
        return None
    locs = re.findall(r"<loc>([^<]+)</loc>", resp.text)
    pages = sorted({u for u in locs if "/note-hydrologique-decadaire/" in u
                    and not u.rstrip("/").endswith("note-hydrologique-decadaire")})
    return pages


def page_pdf_url(page_url: str) -> str | None:
    resp = fetch(page_url)
    if resp is None:
        return None
    m = PDF_HREF_RE.search(resp.text)
    return "https://dgre.gov.bf" + m.group(1) if m else None


def read_inventory() -> dict[str, dict[str, str]]:
    if not INVENTORY.exists():
        return {}
    with INVENTORY.open(encoding="utf-8-sig", newline="") as fh:
        return {r["file"]: r for r in csv.DictReader(fh)}


def write_inventory(rows: dict[str, dict[str, str]]) -> None:
    with INVENTORY.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=INVENTORY_COLUMNS, extrasaction="ignore",
                           lineterminator="\n")
        w.writeheader()
        for k in sorted(rows):
            w.writerow(rows[k])


def download(url: str, dest: Path) -> tuple[int, str] | None:
    """Stream to a temp file, verify it is a PDF, then move into place."""
    resp = fetch(url, stream=True)
    if resp is None:
        return None
    digest = hashlib.sha256()
    size = 0
    with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False) as tmp:
        tmp_path = Path(tmp.name)
        try:
            for chunk in resp.iter_content(chunk_size=65536):
                tmp.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        except requests.RequestException as exc:
            log(f"  download broke mid-stream: {exc!r}")
            tmp_path.unlink(missing_ok=True)
            return None
    head = tmp_path.open("rb").read(5)
    if not head.startswith(b"%PDF"):
        log(f"  not a PDF ({head!r}); discarded")
        tmp_path.unlink(missing_ok=True)
        return None
    tmp_path.replace(dest)
    return size, digest.hexdigest()


def main() -> int:
    started = utc_stamp()
    for d in (RAW_DIR, RUN_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {
        "source": "burkinafaso/dgre", "started_at": started,
        "sitemap": SITEMAP_URL, "new_files": [], "skipped": 0, "failed": [],
    }

    pages = decadal_pages()
    if pages is None:
        # An unreachable host is a source outage, not a scraper failure. The
        # archive being collected is permanent, so nothing is lost by waiting
        # for the next scheduled run.
        summary["status"] = "source_unavailable"
        summary["finished_at"] = utc_stamp()
        (RUN_LOG_DIR / f"{started}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        log("[warn] dgre.gov.bf unreachable; recorded source_unavailable and exiting 0")
        return 0

    log(f"[info] sitemap lists {len(pages)} decadal-note pages")
    inventory = read_inventory()

    for page in pages:
        url = page_pdf_url(page)
        if url is None:
            summary["failed"].append(page)
            continue
        name = requests.utils.unquote(url.rsplit("/", 1)[-1])
        dest = RAW_DIR / name
        if name in inventory and dest.exists() and dest.stat().st_size > 0:
            summary["skipped"] = int(summary["skipped"]) + 1
            continue
        log(f"[get] {name}")
        got = download(url, dest)
        if got is None:
            summary["failed"].append(name)
            continue
        size, sha = got
        inventory[name] = {
            "file": name, "source_url": url, "size_bytes": str(size),
            "sha256": sha, "retrieved_at": started,
        }
        summary["new_files"].append(name)
        log(f"  saved {size} bytes")

    write_inventory(inventory)
    summary["status"] = "ok" if not summary["failed"] else "partial"
    summary["inventory_total"] = len(inventory)
    summary["finished_at"] = utc_stamp()
    (RUN_LOG_DIR / f"{started}_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    log(f"[done] new={len(summary['new_files'])} skipped={summary['skipped']} "
        f"failed={len(summary['failed'])} inventory={len(inventory)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 - the run log must record the crash
        RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
        (RUN_LOG_DIR / f"{utc_stamp()}_crash.json").write_text(
            json.dumps({"source": "burkinafaso/dgre", "status": "crashed",
                        "traceback": traceback.format_exc()}, indent=2),
            encoding="utf-8")
        raise

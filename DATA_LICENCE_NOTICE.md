# Data licence notice and public-release boundary

**Status: owner decision recorded 2026-08-04 — collection continues here;
publication is a separate, human-confirmed step. Read before reusing anything
under `data/`.**

The repository owner has decided that this repository is a *collection* point
for ongoing research, and that any release will be confirmed by a person
first. Scheduled scrapers therefore keep running and committing here.

Two things follow from that decision and are stated plainly so nobody is
misled later:

- This repository is **public**, so everything under `data/` is already
  world-readable and indexable. That is a stronger exposure than local storage,
  independent of anyone's intent to publish.
- Nothing in this repository constitutes permission to redistribute. The table
  below records, per source, what the originating agency actually allows.

This repository holds two different things under two different sets of terms:

- **the scraper code and workflows** — written by this project;
- **the contents of `data/`** — observations retrieved from national water
  agencies, each governed by that agency's own terms, not by this project.

Publishing the code is unproblematic. Republishing the retrieved observations
is only permitted where the originating agency allows it.

## Release assessment per source

Reuse status follows the project's licence register
(`水库数据讨论审计报告.md` and each country's `reservoir_metadata.csv` in the
delivery layer). The governing rule is `SCHEMA_DELIVERY.md` §11:

> Data marked `undeclared_review`, `restricted_use`, `mixed_review`, or
> `prohibited` must not be placed in an unconditional public redistribution
> package without the required review, separation, or written permission.

| `data/` path | Source | reuse_status | May be published as-is? |
|---|---|---|---|
| `japan/opengov` | MLIT via OpenGov.jp | `open_attribution` (CC BY 4.0) | Yes, with attribution |
| `luxembourg/age` | AGE Luxembourg | `open_no_attribution` (CC0 1.0) | Yes |
| `taiwan/wra` | Taiwan WRA via data.gov.tw | `open_attribution` (OGDL v1) | Yes, with attribution |
| `india/apwrims` | CWC / India-WRIS | `attribution_required_review` | Not until the OGDL version is pinned |
| `china/mwr`, `china/mwr_api` | MWR China | `undeclared_review` — source asserts copyright | **No** |
| `malaysia/luas` | LUAS Selangor | `undeclared_review` | **No** |
| `philippines/pagasa` | PAGASA-DOST | `undeclared_review` | **No** |
| `thailand/rid` | Royal Irrigation Department | `undeclared_review` | **No** |
| `southafrica/dws_weekly` | DWS South Africa | `undeclared_review` | **No** |
| `morocco/abhsm` | ABHSM (PDF bulletin) | `mixed_review` | **No** for the PDF-derived fields |
| `malaysia/sarawak_rivers` | Sarawak iHydro | `undeclared_review` | **No** |
| `burkinafaso/dgre` | DGRE Burkina Faso (decadal PDF notes) | `undeclared_review` | **No** |
| `lesotho/lhda` | LHDA Lesotho (homepage dam widget) | `undeclared_review` | **No** |

Counting the nine African deployments listed further down, most data paths
in this repository are in the "no" column, and the repository carries no
licence file of its own.

## Options if the exposure is to be reduced later

Recorded for reference, not as pending actions. Any of these would narrow the
gap between what is reachable and what is licensed:

1. **Make the repository private** (fastest, fully reversible). Scheduled
   scrapers keep working; note that Actions minutes on private repositories are
   metered. This restores compliance immediately without touching history.
2. **Remove the restricted sources' data** from the repository and keep only
   the three open ones, moving the rest to private storage. Note that deleting
   files does not remove them from git history — history rewriting or a fresh
   repository would be required for a clean result.
3. **Obtain written permission** from each agency in the "no" column. This is
   the only route that makes public redistribution durable, and it is worth
   starting for the sources the project depends on most.

None of these is required by the owner's current decision. Treat every `data/`
path except `japan/opengov`, `luxembourg/age`, and `taiwan/wra` as internal
research material that happens to be reachable, not as published data.

## Africa deployments prepared 2026-08

The nine African deployments added in August 2026 (Zimbabwe ZINWA, Namibia
NamWater, Mozambique HCB and ARA-Centro, Ghana Bui and VRA, Tunisia ONAGRI,
Morocco ABHT, South Africa Cape Town WCWSS) are **all `undeclared_review`**.
Under the owner's 2026-08-04 decision they collect on schedule here. Several of
them are overwrite-prone or current-snapshot sources, so pausing them loses
observations permanently - that is why they run rather than wait. Their
`licence_gate` in `config/windowed_sources.json` records that collection is
internal and that redistribution needs a separate human decision.

`burkinafaso/dgre` (added 2026-08-10) is the exception in mechanics but not
in licence: DGRE keeps its decadal notes online (permanent archive), so the
half-monthly workflow is a resilience copy rather than a race against
overwriting, and the source is deliberately not in the windowed-source
registry. Its terms are undeclared like the rest - see the table above.

## Attribution required where publication is permitted

- Japan: "Source: MLIT Water Information System (Suimon-Suishitsu Database),
  republished by OpenGov.jp under CC BY 4.0."
- Taiwan: "Source: Water Resources Agency (WRA), Taiwan, Open Government Data
  License v1 (data.gov.tw)."
- Luxembourg: attribution not required under CC0; retained as good practice.

Last reviewed: 2026-08-10 (burkinafaso/dgre row added; owner decision of
2026-08-04 unchanged).

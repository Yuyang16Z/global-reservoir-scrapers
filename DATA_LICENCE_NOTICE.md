# Data licence notice and public-release boundary

**Status: unresolved — read before making this repository public or reusing
anything under `data/`.**

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

Seven of the eleven data paths currently in this repository are in the "no"
column, and the repository carries no licence file of its own.

## Required decision

One of the following has to be chosen before the current state is acceptable:

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

Until one is chosen, treat every `data/` path except `japan/opengov`,
`luxembourg/age`, and `taiwan/wra` as internal research material that happens
to be reachable, not as published data.

## Africa deployments prepared 2026-08

The seven African sources prepared in August 2026 (Zimbabwe ZINWA, Namibia
NamWater, Mozambique HCB and ARA-Centro, Ghana Bui and VRA, Tunisia ONAGRI) are
**all `undeclared_review`**. Their scrapers and workflows are present, but
their `licence_gate` in `config/windowed_sources.json` states that their data
must stay in private storage. Do not enable them on a public repository.

## Attribution required where publication is permitted

- Japan: "Source: MLIT Water Information System (Suimon-Suishitsu Database),
  republished by OpenGov.jp under CC BY 4.0."
- Taiwan: "Source: Water Resources Agency (WRA), Taiwan, Open Government Data
  License v1 (data.gov.tw)."
- Luxembourg: attribution not required under CC0; retained as good practice.

Last reviewed: 2026-08-04.

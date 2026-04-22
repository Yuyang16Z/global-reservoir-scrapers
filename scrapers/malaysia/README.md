# Malaysia — scrapers overview

All Malaysia-sourced scrapers live under this directory. Each sub-source has its own
scraper folder + output folder; see the per-source READMEs for details.

## Sub-sources

| Folder | Source | Kind | Records | Schedule | Script |
|---|---|---|---|---|---|
| [`luas/`](./luas/) | LUAS IWRIMS (Selangor) | ✅ Reservoir (dynamic) | 8 dams + 1 barrage | 2×/day cron | `luas/malaysia_luas_scraper.py` |
| [`mywater/`](./mywater/) | MyWater Portal — JPS dams | ✅ Reservoir (static metadata only) | 16 dams nationwide | manual trigger | `mywater/mywater_jps_scraper.py` |
| [`sarawak_rivers/`](./sarawak_rivers/) | DID Sarawak iHydro | ⚠️ **River / rainfall, NOT reservoir** | ~269 gauge stations | 2×/day cron | `sarawak_rivers/sarawak_ihydro_scraper.py` |

Output goes to the mirror path under `data/malaysia/<source>/`.

## Known overlaps

- **BATU** appears in both `luas/` (`MY_LUAS_1304`) and `mywater/` (`MY_MYWATER_JPS_BATU`).
  Same physical dam, different source attributes. Downstream normalize step merges them.

## Deferred sources

- **TNB hydro dams** (Kenyir / Pergau / Temengor / Sultan Mahmud / Chenderoh / Sg Perak cascade) —
  TNB publishes no public real-time API. Public Infobanjir may carry a few of their stations
  but returned 403 when probed from GitHub Actions. Revisit when a local IP / VPN is available.
- **Sarawak Energy (Bakun / Murum / Batang Ai)** — Sarawak Energy / SEB publishes only press
  releases, no live feed. These do NOT appear in DID's iHydro.
- **Other state JPS portals** (Pahang / Johor / Kelantan / Perak / Kedah / Penang / Terengganu / Sabah) —
  most are river-stage focused, not reservoirs. Not scoped yet.
- **`hydroportal.water.gov.my`** (Aquarius) — dead, needs auth. Archived.

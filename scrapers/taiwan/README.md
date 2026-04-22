# Taiwan — scrapers overview

## Sub-sources

| Folder | Source | Kind | Records | Schedule | Script |
|---|---|---|---|---|---|
| [`wra/`](./wra/) | Water Resources Agency (WRA) open data + disaster-prevention APIs | Reservoir data (dynamic) | Nationwide announced reservoirs | 2×/day cron | `wra/taiwan_wra_scraper.py` |

Output goes to `data/taiwan/<source>/`.

# Morocco — scrapers overview

## Sub-sources

| Folder | Source | Kind | Records | Schedule | Script |
|---|---|---|---|---|---|
| [`abhsm/`](./abhsm/) | ABHSM Souss-Massa daily barrage situation PDF | Reservoir snapshot + raw PDF | 9 dams, daily current-state snapshot | 2×/day cron | `abhsm/morocco_abhsm_scraper.py` |

Output goes to `data/morocco/<source>/`.

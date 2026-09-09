# Sri Lanka — Mahaweli Authority reservoir bulletin

47 reservoirs. The Water Management Secretariat publishes a two-page
"Latest Status of Reservoirs" PDF at a **fixed URL that is overwritten with each
issue**:

    https://mahaweli.gov.lk/WMS%20DATA/Menue-WMS%20-%20E.pdf

## Why this is scheduled daily

Retention is `overwrite_prone` at daily cadence. There is no dated archive on the
site and, verified 2026-09-08, **the Internet Archive does not hold the bulletin**:
its 482 archived PDFs under `mahaweli.gov.lk` are tenders, annual reports and
gazettes, not the WMS status sheet. A missed day is therefore lost permanently and
cannot be recovered later.

The delivered series shows what that costs. Before this deployment the country held
47 reservoirs with **one row each**, because nothing was capturing the fixed URL
between manual runs.

Variables: `water_level_masl` (as printed, msl), `storage_mcm`, `storage_pct`
(percent of gross capacity), plus last-24h rainfall and spill where published.

Licence: `undeclared_review`. Public commit to this world-readable repository was
authorised by the project owner on 2026-09-09. Attribution: Water Management
Secretariat, Mahaweli Authority of Sri Lanka.

# Japan — Kanagawa Prefecture dams (kanagawa-dam.jp JSON API)

Five prefecture-managed reservoirs, none of which appear in the MLIT/OpenGov Japan
delivery: 相模ダム, 城山ダム, 宮ヶ瀬ダム, 三保ダム, 道志ダム.

Variables per reservoir: `water_level_m`, `storage_mcm`, `storage_pct`,
`total_inflow_m3s`, `total_outflow_m3s` — a fuller set than the servlet family, which
mostly lacks inflow/outflow for some prefectures.

## Only the latest point is taken, and why

`/api/summary.php` returns 57 series, each with 30 indexed values and a `dt` field.
**`dt` is not a per-series observation time**: across the 57 series it decrements by
exactly one hour in listing order (2026-09-09 18:00 down to 2026-09-07 10:00), which
is a generator cursor. The spacing of the 30 points cannot be established from the
API, so dating them would fabricate a time axis.

The **last** index is verifiable as the current value, cross-checked against the
sibling endpoint `/api/water-storage-level.php`:

| Reservoir | sibling endpoint | last index |
|---|---|---|
| sagami | 76 | 75.82 |
| miyagase | 99 | 99.00 |
| miho | 84 | 84.30 |

That value is taken and dated with the payload's own `lastUpdate`. Recovering the
30-point history requires establishing the real time axis first; it is left unread
rather than guessed.

## Exclusions

- **Aggregate series** (`all_*`, `sagamisum_*`, `sagamiko_and_*`) sum several
  reservoirs and are not single water bodies.
- **`*_ratio`** is a second percentage on an undocumented basis — `sagamiko_ratio`
  reads 91.90 where `sagamiko_storage_level` reads 75.82 for the same object — so it
  is not delivered.
- **Lake-side series** (`sagamiko_*`, `tsukuiko_*`, `miyagaseko_*`, `tanzawako_*`)
  duplicate the dam-side volume exactly and are the same objects.

## Datum

`water_level_m`, not `_masl`. 三保ダム reads -8.50 where the others read 118–310, so at
least one gauge uses a local datum and the source documents none.

## Licence

`undeclared_review`. Public commit to this world-readable repository was authorised by
the project owner on 2026-09-09. Attribution: Kanagawa Prefecture (kanagawa-dam.jp).

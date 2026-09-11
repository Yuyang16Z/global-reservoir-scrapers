# Japan — prefectural / JWA dam tables served as plain HTML

Three sites that publish a dam table directly in HTML and do **not** share the
`servletBousaiTableStatus` platform. Each carries an explicit, hand-verified column
configuration.

| Site | Dams | Variables | Cadence |
|---|---:|---|---|
| Fukushima 福島県 | 11 | `water_level_m` | hourly |
| JWA Tone–Arakawa 利根川・荒川水系 | 13 | `storage_mcm`, `storage_pct_usable` | daily (0時) |
| Hyogo 兵庫県 | 10 | `storage_mcm`, `storage_pct_usable` | daily |

## Why the configuration is explicit rather than generic

A generic header matcher was written first and **rejected**. On these pages it:

- mapped Fukushima's `下流水位` — the **tailwater** level below the dam — onto
  `water_level_m` alongside the reservoir level;
- mapped JWA's `前日貯水量` (**yesterday's** volume) into the same column as today's,
  so one row would have carried two dates;
- treated Hyogo's `利水容量` and Mie's `有効貯水量` — **static design capacities** —
  as observations.

Column indices are therefore pinned per site, and the excluded columns are named in
the config so a future edit cannot silently re-admit them.

## Other deliberate choices

- **`storage_pct_usable`, not `storage_pct`.** Both sites compute the percentage
  against usable capacity, not gross. Verified arithmetically: Hyogo
  13,026 / 13,300 = 97.9 %; Mie 3,616 / 3,716 = 97.3 %.
- **Static capacities go to `reference_values.csv`**, never into the series.
- **Mie 三重県 is excluded.** Its page publishes no observation timestamp. Stamping
  rows with fetch time would fabricate an observation date, so its 6 dams are left
  out until a real timestamp is found.
- **JWA publishes `9月8日0時現在` with no year.** The year is taken as the current one,
  stepping back by one if that would place the observation in the future.

## Licence

`undeclared_review`, as for the servlet family. Deployment to this public repository
was authorised by the project owner after the world-readable exposure was stated
explicitly. Attribution: Fukushima Prefecture river information system; Japan Water
Agency; Hyogo Prefecture.


## Known issue: empty table cells were discarded (fixed 2026-09-10)

Until the commit that added this section, `cells()` discarded empty table cells before
values were read by the configured column positions. A blank cell before a mapped column
would therefore have moved that row's later values one column to the left without any
error. The same defect was confirmed in the servlet-family scraper, where it did corrupt
rows.

No shifted row was found here: re-parsing the four latest raw pages, the old parser, the
fixed parser and the archive agree cell for cell (Fukushima, JWA Tone-Arakawa, Yamaguchi,
Hyogo), and keeping empty cells leaves every configured column index correct on those
pages. A transient blank in an earlier run cannot be ruled out, because the raw pages are
overwritten on every run, so the formatted delivery does not use rows observed before
this fix reached a scheduled run.

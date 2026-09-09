# Japan — Miyagi Prefecture dams (monthly storage-status PDF)

17 prefecture-managed dams with reservoir level (m above sea level) and storage as a
percentage of **usable** capacity, published as one PDF per issue.

## Retention

`overwrite_prone`. The filename carries a Reiwa-era date (`r80901_tyosui.pdf` =
令和8年9月1日) but older dates are removed — verified 2026-09-09 that r80801, r80701,
r80601 and r80815 all return 404. The Internet Archive holds only three snapshots,
under three different naming schemes (`damutyosuii.pdf` 2022, `tyosui.pdf` 2024,
`r7_7tyosuii.pdf` 2025), which is not a usable backfill. A missed issue is lost.

The PDF link is read from the landing page rather than built from a guessed date,
because the naming convention has changed at least twice.

## Exclusions

- **過去10年平均の貯水位 / 過去10年平均の貯水率** — ten-year climatological means, not
  observations of the issue date. These sit in columns 3 and 4 of the same table and a
  positional parser that took "the numbers" would silently deliver climatology as data.
- **水位順位** — a rank, not a measurement.
- **The 全体 row** — an all-dam aggregate, not a water body.

## Units

`storage_pct_usable`, not `storage_pct`: the sheet names its own denominator as
利水容量 (現在の貯水量／利水容量). Values above 100 % are genuine — 長沼 reads 132.3 %.

## Licence

`undeclared_review`. Public commit authorised by the project owner on 2026-09-09.
Attribution: Miyagi Prefecture (pref.miyagi.jp).

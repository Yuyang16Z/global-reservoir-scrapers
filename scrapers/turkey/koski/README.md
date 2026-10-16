# turkey/koski - KOSKİ dam readings (Konya, Turkey)

**Source:** one page per dam on the website of KOSKİ (Konya Su ve Kanalizasyon İdaresi), the water and sewerage
administration of Konya:
<https://www.koski.gov.tr/koski/altinapa-baraj-degerleri>, `.../bagbasi-baraj-degerleri`,
`.../bozkir-baraj-degerleri` and `.../afsar-baraj-degerleri`.

**What is captured:** the readings table (id `databaraj`) of each page, cell text as published: 'Tarih' (date,
DD/MM/YYYY, stored as `measurement_date`), 'Yükseklik' (height: a gauge reading in cm at Altınapa, the level in whole
metres at the other three dams), 'Kot' (water-surface elevation, m), 'Hacim' (volume, m3) and 'Doluluk Oranı' (fill
ratio, %). Columns are read by their header names: Internet Archive captures from 2020 print 'Gelen CM' and
'Gelen LT' (incoming) where today's pages print 'Doluluk Oranı', and those columns are kept under their own names.
Nothing is converted, cross-checked or blanked here; the project's delivery layer does that.

**Retention:** `rolling_window`. On the captures of 2026-09-08, 2026-09-29 and 2026-10-14 the Altınapa and Bağbaşı
tables started six years before the capture day in Istanbul time (2020-09-08, 2020-09-30, 2020-10-14), so an early
date leaves the page every day. Bozkır and Afşar start at 2022-01-01, inside that window so far. No published value
changed between those captures. The site serves no other copy of the dates that have left the pages.

**Schedule:** Mondays 14:40 Europe/Istanbul (11:40 UTC). Each run reads the four pages, 3 s apart. A date stays
visible for about 313 weekly runs, so a missed or late run loses nothing.

**Storage:** a page is stored (`raw/<year>/<slug>_<capturedUTC>__<hash12>.html.gz`) once per distinct table of
that dam (hash of the table cells, not of the HTML); a page without a readable table goes to `raw/unparsed/` once
per distinct content and the run is marked partial. `raw/captures.csv` lists every stored page.
`timeseries/turkey_koski.csv` holds the latest capture of each (dam, date) and `timeseries/turkey_koski_revisions.csv`
every value that changed between captures, both rebuilt from the stored pages on every run. HTTP 403/412/429 stops
the run.

**Seed and imports:** the archive was seeded on 2026-10-15 with the project's own captures of 2026-09-08, 2026-09-29
and 2026-10-14 (origin `local_capture`; 3,682 dam-dates from 2020-09-08, of which 58 had already left the pages).
Saved pages named `<slug>_<capture time UTC>.html` can be added with `IMPORT_DIR` (and `IMPORT_ORIGIN`, e.g.
`internet_archive`), for instance Internet Archive captures of the four pages (2020-2026).

**Licence:** KOSKİ publishes no reuse or redistribution terms; the footer reads "© 2026 | www.koski.gov.tr Tüm
Hakları Saklıdır" (all rights reserved) - `undeclared_review` (checked 2026-09-08). Collected here by the owner's
decision of 2026-10-15; see `DATA_LICENCE_NOTICE.md`. Presence in this repository is not permission to republish.

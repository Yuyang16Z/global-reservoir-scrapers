# Zambia/Zimbabwe - ZRA Lake Kariba

Captures the two hydrology tables the **Zambezi River Authority** publishes for
Lake Kariba at <https://www.zambezira.org/>.

Kariba is among the largest reservoirs in the world by volume and is shared
between Zambia and Zimbabwe; ZRA operates it jointly on behalf of both.

## Two tables, two retention classes

| page | what it gives | retention |
|---|---|---|
| `/hydrology/lake-levels` | daily lake level and % usable storage | **rolling window**, about a fortnight |
| `/hydrology/kariba-reservoir-data` | one month across three years; the current year also carries live storage, turbine discharge, spillage and total outflow | monthly, replaced when the month turns |

The daily table is the lossy one: it drops the oldest day as a new one arrives,
so anything not captured within two weeks is gone. That is what the twice-weekly
schedule is for. The monthly table is replaced when the month turns. Numbered
pages kept older months only up to May 2021 (the newest the project's 2026-09
crawl of the site found), so a later day's storage and outflows survive only in
captures taken while its month was on the page.

## Values printed late

ZRA prints a day's turbine discharge, spillage and total outflow a day or more
after the day's level, and posts the monthly table in batches, so the first
capture of a day often lacks them. Every run therefore replays all raw
captures, oldest first:

- a new date is added with the stamp of the capture that first printed it
  (`fetched_at`, never changed afterwards);
- a blank cell takes the value of the first later capture that prints it, and
  the run summary lists each filled cell (`filled`, `daily_filled`,
  `monthly_filled`);
- a value already held is never changed or blanked; a capture that prints it
  differently is listed in `zra_kariba_revisions.csv` (ZRA restates days). A
  difference of printed precision only, such as 39 against 38.64, is not a
  revision.

Up to 2026-10-15 a date was written once and never updated, so outflows printed
after the first capture were dropped: 2026-09-07, 09-16 and 10-05 had them
in later captures and were filled by the first replay. Values ZRA had not
printed when their month was replaced are lost: the outflows of 2026-08-31 and
09-28, and the storage and outflows of 09-29 and 09-30.

Before this scraper existed the project had no automation for Kariba at all,
and the delivery had been rebuilt by hand from whatever the monthly page
happened to show - which is why 2022 and 2023 were missing from it entirely.

## Parsing notes

- The daily table prints `18-Aug` with **no year**. The year comes from the
  page's own From/To fields, and is assigned per row by walking that range,
  because a fortnight can cross a year boundary.
- Column positions in the monthly table are **derived from the header**: the
  earlier years occupy a level and a percentage each, so whatever follows
  `day + 2*(years-1)` cells is the current year's block. Assuming a fixed width
  silently produced zero rows when the table carried 11 columns rather than 12.
- The year columns are named `September 2026` or, as on 2026-09-24,
  `Sep-26`. Only the first form was read until 2026-10-15, so that capture
  parsed to nothing; replaying it now fills 2026-09-16's outflows from it.
- October's table prints its 31st row with an **empty day cell**. A row with an
  empty day cell straight after day 30 of a 31-day month is read as day 31: in
  the 2026 October page that row's 2024 cells (476.14 m, 4.46%) are the values
  a 2025-11-01 capture printed under a `31` label.
- Percentages are **% usable storage** against the 475.50-488.50 m operating
  range, not against total capacity. They are recorded as the source states
  them and must not be compared with a %-of-capacity figure from elsewhere.

## Outputs

```
data/zambia/zra_kariba/
  raw/lake_levels_<stamp>.html, reservoir_data_<stamp>.html
  timeseries/zambia_zra_kariba_daily.csv     merged, idempotent
  timeseries/zambia_zra_kariba_monthly.csv   merged, idempotent
  zra_kariba_revisions.csv                   held values a later capture printed
                                             differently (written once one occurs)
  run_logs/                                  status=source_unavailable on outage
```

## Licence

Undeclared - ZRA publishes the tables with no reuse statement. Recorded as
`undeclared_review` in `DATA_LICENCE_NOTICE.md`: internal research use only.

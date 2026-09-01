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
schedule is for. The monthly table's older months stay reachable as numbered
pages, so history there is recoverable, but capturing on the 1st avoids relying
on that.

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
- Percentages are **% usable storage** against the 475.50-488.50 m operating
  range, not against total capacity. They are recorded as the source states
  them and must not be compared with a %-of-capacity figure from elsewhere.

## Outputs

```
data/zambia/zra_kariba/
  raw/lake_levels_<stamp>.html, reservoir_data_<stamp>.html
  timeseries/zambia_zra_kariba_daily.csv     merged, idempotent
  timeseries/zambia_zra_kariba_monthly.csv   merged, idempotent
  run_logs/                                  status=source_unavailable on outage
```

## Licence

Undeclared - ZRA publishes the tables with no reuse statement. Recorded as
`undeclared_review` in `DATA_LICENCE_NOTICE.md`: internal research use only.

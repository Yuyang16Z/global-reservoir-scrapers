# Japan — Shimane Prefecture dams (hourly JSON)

19 prefecture-managed dams. **The densest Japanese prefectural source deployed so far**:
unlike the others it publishes a real intra-day series, not a single current value.

    https://www.suibou-shimane.jp/dyn/dps/json/<YYYYMMDD>/dam60.json

The day file holds every hourly observation of that day since 00:00 JST. Past day
files stay up for a few days (checked 2026-10-06: the three previous days complete;
20260929 and the dates checked on 2026-09-10 404). Every run also re-reads the last
three days (`DAYS_BACK`), so a day's evening hours are filled in by the next morning's
runs instead of depending on a run just before midnight.

## Field codes were decoded, not guessed

The payload uses opaque codes. They were resolved by matching against the rendered
ダム諸量一覧表, using dams whose inflow differs from outflow to break the ties:

| code | source column | delivered |
|---|---|---|
| `7_10` | 貯水位 [EL.m] | `water_level_masl` |
| `7_50` | 流入量 [m3/s] | `total_inflow_m3s` |
| `7_70` | 全放流量 [m3/s] | `total_outflow_m3s` |
| `7_20` | 貯水量 [1000 m3] | `storage_mcm` (×0.001) |
| `7_41` | 利水貯水率 洪水期 [%] | `storage_pct_flood_season` |
| `7_42` | 利水貯水率 非洪水期 [%] | `storage_pct_nonflood_season` |

**Not delivered.** `7_30` 空容量 and `7_200` 空容量率 are the complement of storage
rather than an independent measurement. `7_80` and `7_120` could not be identified — for
布部ダム they read 0.00 and 4.22 against a published 全放流量 of 4.22 — so their meaning
is unestablished and guessing would mislabel a variable.

The two 利水貯水率 columns use **different seasonal denominators** and which one applies
depends on the date, so both are carried under distinct names rather than merged.

## Station names

The site publishes no station master. The id→name map was derived by matching each id's
貯水位 at 2026-09-10 08:30 against the rendered table at the same timestamp: all 19
matched uniquely, zero ambiguity. An unmapped id is still emitted with its raw code, so
a newly added dam is visible rather than silently dropped.

## Missing values

Arrive as Japanese words — 未収集 (not collected), 欠測 (missing) — and are never coerced
to numbers. Coverage of the two percentage columns is consequently lower than of level
and flow.

## Licence

`undeclared_review`. Public commit authorised by the project owner on 2026-09-09.
Attribution: Shimane Prefecture 水防情報システム (suibou-shimane.jp).

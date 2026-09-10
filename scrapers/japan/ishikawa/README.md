# Japan — Ishikawa Prefecture dams (hourly JSON)

12 dams with an intra-day hourly series. Same vendor platform as Shimane, different
file layout and JSON shape:

    /dyn/dps/timeline/<YYYYMMDD>/<YYYYMMDD>_1_dam_60.json

    {"<station_id>": {"data60": [{"item_10": {"val": ...}, ..., "time": ...}]},
     "updateTime": ..., "observationTime": ...}

The day file resets at midnight and past dates 404, so the late-day run is primary and
the earlier ones are insurance.

## Field codes were verified here, not inherited

Ishikawa shares Shimane's numeric codes, but the mapping was checked against **this
site's own** rendered ダム諸量概況図 rather than assumed from the sibling:

| code | column | 手取川 in JSON (08:00) | rendered (08:50) |
|---|---|---|---|
| `item_10` | 貯水位 [EL.m] | 447.09 | 447.12 |
| `item_50` | 流入量 [m3/s] | 51.02 | 51.02 |
| `item_70` | 全放流量 [m3/s] | 0.49 | 0.49 |

`item_20` (貯水量, 1000 m³) is not shown on the overview page and so could not be
cross-checked directly; it is carried on the strength of the shared code plus a
plausible magnitude — 手取川 107,752 → 107.75 mcm against a published capacity of
roughly 231 mcm.

## Overlap with the MLIT delivery

**手取川ダム is already delivered** from MLIT/OpenGov (matched by both Japanese name and
romanised code); the site itself labels it 手取川ダム(国), nationally managed. It must be
de-duplicated at the delivery layer rather than admitted twice. The other **11 dams are
new**. Where they coincide, this feed is hourly while the MLIT feed is daily.

## Station names

Derived by matching each id's `item_10` against the rendered overview at a shared
timestamp: 12/12 unique, zero ambiguity. An unmapped id is still emitted with its raw
code and listed in the run log, so a newly added dam cannot be silently dropped.

## Value handling

Numbers arrive as strings with thousands separators (`"107,752"`) and missing readings
as `"-"`; both are handled explicitly and never coerced.

## Licence

`undeclared_review`. Public commit authorised by the project owner on 2026-09-09.
Attribution: Ishikawa Prefecture 河川総合情報システム (kasen.pref.ishikawa.lg.jp).

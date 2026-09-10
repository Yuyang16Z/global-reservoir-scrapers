# Japan — 川の防災情報 (river.go.jp/kawabou), national dam portal

**912 dam stations across all 47 prefectures**, in one API: MLIT, JWA, prefectural,
municipal-utility and power-company dams alike. Roughly **583 are new** against
everything this project already holds for Japan.

## How it was found, and why late

It was reached on the eighth prefectural site, by following a link from the Tokyo
Waterworks page (小河内貯水池 → "リアルタイム貯水量情報（国土交通省「川の防災情報」）").
The MLIT link directory that seeded this sweep lists the 47 *prefectural* systems and
never points at this portal, so a link-directory-first approach could not surface it.
Seven per-prefecture scrapers had already been built by then.

**Those seven are not made redundant.** They publish 貯水量 and 貯水率 for prefectural
dams that this portal almost always withholds for the same dams, and they carry ~30
dams it does not list at all. This portal is the wider net; they are the deeper one.

## Endpoints

All read from the site's own Vue bundle (`js/app.*.js`), not guessed:

| endpoint | source function |
|---|---|
| `file/files/tmlist/past/dam/<YYYYMMDD>/<obsFcd>.json` | `getSelectedTmDamPast()` |
| `file/files/tmlist/dam/<YYYYMMDD>/<HHmm>/<obsFcd>.json` | current values |
| `file/files/master/obs/dam/<obsFcd>.json` | station metadata |
| `file/files/obslist/idx/pref/twn/<prefCd>.json` | towns, with `damExistFlg` |
| `file/files/obslist/obs/twnlist/<twnCd>.json` | stations in a town |
| `file/system/tmCrntTime.json` | the clock the site builds `<HHmm>` from |

`obsFcd` is `ofcCd`(5, zero-padded) + `itmkndCd`(2, `07` = dam) + `obsCd`(5, zero-padded).

Station enumeration walks `prefarea.json` → per-prefecture town index → per-town station
list, and is kept in `stations.json`; `enumerate_stations.py` regenerates it.

## The two windows have different depths

| endpoint | span | fields |
|---|---|---|
| `past` | **169 hourly points, 7 days** | 貯水位, 流入量, 全放流量 |
| current | 50 hourly points, ~50h | the above **plus** 貯水量 and the percentages |

So the week-deep window is three variables wide, and the storage series is only two
days deep. Both are fetched per station and merged on `(obs_fcd, observed_at)`. The
schedule is daily because the shorter window dictates it.

## Quality codes are not optional

**A missing reading is published as `0` with a code, never as `null`.** 佐賀県河内ダム
returns `storLvl: 0.0, storLvlCcd: 140` — 閉局, the station is closed. Taking the value
without reading its code writes a 0 m water level for a decommissioned dam.

The threshold used here is the app's own, applied identically to stage, rainfall,
water-quality and dam fields throughout its source:

```
valid  <=>  value is not None and ccd is not None and ccd < 128
```

`190` 欠測 (missing) · `140` 閉局 (closed) · `160` not measured — all ≥ 128.

## Fields

| JSON | delivered as | unit |
|---|---|---|
| `storLvl` | `water_level_m` | m above dam datum |
| `storCap` | `storage_mcm` | 1000 m³ → ×0.001 |
| `allSink` | `total_inflow_m3s` | m³/s |
| `allDisch` | `total_outflow_m3s` | m³/s |
| `storPcntIrr` | `storage_pct_usable` | % |

Prefectural and utility dams generally publish level, inflow and outflow only;
storage and the percentages arrive coded ≥128 and are dropped.

## Verification

小河内ダム read here as `storCap` 140,190 (1000 m³) at `storPcntIrr` 75.6 %, against
**Tokyo Waterworks' own page** reporting 14,141.6 万m³ at 76.3 % for the same reservoir
at a nearby timestamp on the same day — two independent publishers agreeing on both the
value and the unit conversion. Its `storLvl` 515.06 m also sits inside the master
record's own `minStg` 425 / `nrmlHighStg` 526.5 range.

## Known data issues — for the delivery layer, not fixed here

The archive keeps what the portal published, removing only values the portal itself
declares non-observations (ccd ≥ 128). What follows was found in the first full capture
(2026-09-10: 873 stations with data, 159,851 rows). Each item is a judgement, so it is
applied when this feed is formatted for delivery, not baked into the archive.

**Water level 0.0 under a valid code is a sentinel — provably, station by station.**
Nine stations publish 0 m with ccd 0 while their own master record puts the minimum
stage far above zero, which makes the reading physically impossible:

| station | zeros | pattern | non-zero median | master minStg |
|---|---|---|---|---|
| 二庄内ダム (青森) | 185 / 185 | the whole window | — | 335.5 m |
| 恵岱別ダム (北海道) | 55 / 185 | one 55-hour block | 196.06 m | 193.92 m |
| 幕別ダム (北海道) | 14 / 185 | 13 separate dropouts | 72.71 m | 65.5 m |
| 日新ダム (北海道) | 11 / 185 | one block | 295.03 m | 291.2 m |
| 西郷ダム (福島) | 6 / 185 | 6 separate dropouts | 639.73 m | 628 m |
| ペーパン, 当麻, 美生 (北海道); 新宮川 (福島) | 1 each | single points | 202–500 m | 197–482 m |

Rule: blank `water_level_m == 0.0`, logging each case in `source_anomalies.csv`, where `min_stage_m` is well above zero (the lowest
among these nine is 65.5 m). A low-datum dam, where a reading near zero could be
physical, needs its own series checked instead. 藤ノ平ダム (佐賀) reads 0.00 for all 185
points but has no stage range in its master record, so it cannot be proved the same
way — log it in `source_anomalies.csv` rather than keep it silently.

**Storage 0 is real — do not blank it.** Six stations report `storage_mcm` 0 across the
whole current window. Beside water levels tens or hundreds of metres above datum, that
first looked like the same kind of sentinel. It is not: `storCap` is **effective**
storage above the operating floor, and at each of the five with a concurrent level, the
level sits below that floor or within half a metre of it:

| station | mean level | master minStg | level − floor |
|---|---|---|---|
| 清瀧ダム (島根) | 31.21 m | 35.3 m | −4.09 m |
| 綱取ダム (岩手) | 181.29 m | 183 m | −1.71 m |
| 日向神ダム (福岡) | 273.89 m | 275 m | −1.11 m |
| 大峠ダム (島根) | 99.44 m | 99.1 m | +0.34 m |
| 嵯峨谷ダム (島根) | 254.35 m | 253.95 m | +0.40 m |

The first three are provably consistent with zero usable storage; the last two are
plausible. None is provably a sentinel, and a late-summer drawdown is exactly when this
happens. (芦別ダム has no concurrent level and cannot be judged.) Blanking these would
delete true observations.

**Inflow zeros are real — keep.** 303 stations report some zero inflow, but only 4 hold
it for the whole window, and none of those fails a mass-balance test (sustained outflow
with a level that does not fall). Small catchments genuinely read near zero.

**Usable-storage % outside 0–100 is real — keep.** A negative value means the level is
below the usable floor: 黒杭川ダム −3.3 %, 江永ダム −1.4 %, and 猫山ダム down to −60 %
with 0.022 mcm stored — extreme, but it moves (−60 → −50) instead of repeating a round
number. Values above 100 % are surcharge above normal high water; Tokyo Waterworks' own
page shows 薗原ダム at 131.7 %.

**Not every station is a reservoir.** `itmkndCd` 7 covers any facility with dam-type
telemetry. By name, 34 stations (27 with data, 3.7 %) are weirs (堰, 大堰), headworks
(頭首工), an injection point (注水口), a retarding basin (遊水地) or a natural lake
(余呉湖); a few more without a telling suffix look like flood-retarding basins
(権現堂調節池, 荒川第一調節池) or a sluice gate (釜口水門). 調整池 is not a safe exclusion —
白丸調整池 and the 調整池ダム entries are regulating reservoirs behind dams. The delivery
layer's reservoir-scope audit decides; the archive keeps them.

## De-duplication — key on obs_fcd, never on name

Japanese dam names repeat across the country: there is a 坂本ダム in Gunma and another
elsewhere. Overlap with existing holdings is 120/127 of the MLIT/OpenGov delivery and
181/211 of the seven per-prefecture feeds, but that matching was by name and is only
approximate. Any delivery-layer de-duplication must key on `obs_fcd` together with
prefecture and coordinates, which `stations_master.csv` carries.

## Storage layout

```
data/japan/kawabou/
  timeseries/hourly/japan_kawabou_hourly_YYYY-MM-DD.csv.gz   one file per JST observation day
  metadata/japan_kawabou_dams.csv                             per-station metadata, fetched once
  run_logs/YYYYMMDD_HHMMSS_summary.json                       one per run, with status
```

- **One file per observation day**, following the repo's own convention
  (`timeseries/daily/…_YYYY-MM-DD.csv` in the Taiwan, India and OpenGov feeds). Once a day
  leaves the 7-day window its file never changes again, so git stores it once instead of
  re-storing a growing file on every run.
- **Gzipped**, unlike those feeds, because of volume: about 20,600 rows a day — roughly
  1.5 MB a day as CSV against 130–190 KB compressed, i.e. ~550 MB a year versus ~55 MB.
- **Written deterministically.** `gzip.open()` stamps the current time into the gzip
  header, so identical rows came out as different bytes on every run — a new blob for every
  file in the window, daily, with no data change behind it. Partitions are written with
  `mtime=0`, and only when their *decompressed* content differs, so a zlib version change on
  the runner cannot trigger a rewrite either.
- **The dated filename is load-bearing.** `scripts/monitor_source_freshness.py` looks for
  dates in `*.csv` cells and falls back to dates in filenames; it does not open `.csv.gz`.
  Rename the partitions and this source silently drops out of monitoring.
- **`run_logs/`** holds each run's `status` (`success` / `partial` / `failed`) with
  `started_at` and `finished_at`, which the monitor reports beside the observation date.

## Licence

Undeclared — the portal publishes no reuse terms. Attribution: 国土交通省 川の防災情報.
Recorded, not gating: collection proceeds under the project owner's 2026-09-09 decision.

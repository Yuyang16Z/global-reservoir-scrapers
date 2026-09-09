# Japan — prefectural dams (`servletBousaiTableStatus` family)

Eight prefectural river/flood-information systems run the same vendor platform and
answer an identical servlet call, so one scraper serves all of them.

| Prefecture | Dams | Prefecture | Dams |
|---|---:|---|---:|
| Niigata 新潟 | 20 | Oita 大分 | 10 |
| Yamagata 山形 | 17 | Aomori 青森 | 10 |
| Fukui 福井 | 13 | Tokushima 徳島 | 7 |
| Miyazaki 宮崎 | 13 | Nara 奈良 | 5 |

**94 dams, 84 of them absent from the existing MLIT/OpenGov delivery.** The other 10
are nationally managed dams that prefectural portals also display; for those this feed
still adds `water_level_m`, which the MLIT feed does not publish for any Japanese dam.

## Why this is scheduled hourly and cannot be backfilled

Retention is `current_snapshot`. Verified 2026-09-09:

- historical queries (`nw=0` with every `tm` format tried) return zero dam rows;
- the Wayback Machine holds **no** snapshot of this servlet for any of the eight
  sites — crawlers never reach it, because the table is loaded into a frame by JS.

There is no backfill path of any kind. Every observation not captured on schedule is
lost permanently, which is why the cadence is hourly rather than daily.

## Parsing notes (do not "simplify" these away)

- **Column order differs per prefecture** even though the servlet is shared.
  Oita is `貯水位|流入量|貯水量|貯水率|放流量`; Yamagata is
  `流入量|全放流量|貯水位|貯水量|貯水率`. Columns are therefore mapped **by header
  label**. A positional mapping silently writes 流入量 into `water_level_m`.
- **Timestamps come in three layouts**: `YYYY MM/DD HH:MM` per row, `MM/DD HH:MM`
  per row without the year, and a table-level `YYYY年MM月DD日HH時MM分 現在` caption.
- **Static design values share the table** and are excluded: 平常時最高貯水位,
  平常時最高水位, 洪水時最高水位, サーチャージ水位, 洪水貯留準備水位, 計画高水流量,
  洪水流入量, 洪水量, 常時満水位. They are reference values, never observations.
- **`storage_mcm` mixes three different quantities** across sites — 貯水量,
  有効貯水量 (usable), 総貯水量 (gross). The exact source label is kept per row in
  `source_header` and must be used to split the column before delivery.
- Encoding is Shift_JIS.

## Licence

Not established. The dam subsites publish no terms and the parent prefectural portals
did not yield a machine-readable licence on 2026-09-09. Recorded as
`undeclared_review`. Deployment to this public repository was authorised by the project
owner on 2026-09-09 after the world-readable exposure was stated explicitly.
Attribution: prefectural river/flood information systems of Niigata, Yamagata, Fukui,
Miyazaki, Aomori, Oita, Tokushima and Nara Prefectures, Japan.

Source discovery: MLIT's official national dam-information link directory,
<https://www.mlit.go.jp/mizukokudo/mizsei/mizukokudo_mizsei_fr2_000008.html>.

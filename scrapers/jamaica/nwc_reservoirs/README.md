# jamaica/nwc_reservoirs - NWC dam and reservoir levels (Jamaica)

**Source:** <https://www.nwcjamaica.com/reservoir.php> - "Dam & Reservoir Levels" page of the National Water
Commission (NWC), Jamaica.

**What is captured:** the latest weekly reading of the two main storage sources of the Kingston & St Andrew supply,
Mona Reservoir and Hermitage Dam: stored volume in million imperial gallons (`storage_mg`), percentage of capacity
(`storage_pct`), capacity in MG and megalitres, the reading date ("As of: 01 Oct 26" -> `observation_date`
2026-10-01), the status label and the trend against the previous reading. The page calls the readings weekly, but
NWC posted readings dated 1, 4 and 5 October 2026.

**Retention:** `current_snapshot`. Each reading replaces the previous one, sometimes within a day; nothing on the
site serves earlier readings. The Internet Archive holds 30 distinct captures of the page (2021-08-27 to 2026-07-11).
The parser reads the 2026 card layout and the earlier layout (one `<section>` per reservoir, "Weekly Summary",
"Last Updated Reading Jul 08, 2026"); a page it cannot read is kept under `raw/unparsed/` and the run is marked
`partial`.

**Schedule:** 02:40, 08:40, 14:40 and 20:40 America/Jamaica (07:40, 13:40, 19:40 and 01:40 UTC).

**Storage:** a page is stored (`raw/<year>/<capturedUTC>__<hash12>.html.gz`) only when its parsed readings are new.
`timeseries/jamaica_nwc_reservoirs.csv` is rebuilt from the stored pages on every run (one row per reservoir and
reading date; a later capture wins) and `timeseries/jamaica_nwc_reservoirs_revisions.csv` lists values that changed
between captures.

**Licence:** NWC's copyright notice (<https://www.nwcjamaica.com/copyright.php>) allows personal and non-commercial
public use and reproduction without charge, provided the copy is accurate, NWC is identified as the source and the
copy is not presented as official or endorsed; reproduction for commercial redistribution needs NWC's written
permission - `attribution_required_review`. Collected here by the owner's decision of 2026-10-04; see
`DATA_LICENCE_NOTICE.md`. Presence in this repository is not permission to republish.

# turkey/izsu - İZSU dam water status (İzmir, Turkey)

**Source:** the open-data API of İzmir Büyükşehir Belediyesi, dataset "Barajların Doluluk Oranları"
(<https://acikveri.bizizmir.com/dataset/barajlarin-doluluk-oranlari>):
`https://acikveriwapi.izmir.bel.tr/AcikVeriIzsuApi/DamWaterStatus` (latest day), `?year=YYYY` (every day of a year)
and `?date=YYYY-MM-DD` (one day). İZSU (İzmir Su ve Kanalizasyon İdaresi) is the water and sewerage administration of
İzmir.

**What is captured:** per facility and date, `waterStatus`, `usableWaterStatus`, `occupancyRate` and `waterLevel`, as
the text the API sent. The dataset describes them as the dams' daily water volume, fill ratio and water elevation; the
answer itself states no units. On 2026-11-27 the latest day listed five dams (Tahtalı, Balçova, Ürkmez, Gördes,
Alaçatı Kutlu Aktaş); earlier years list six or seven facilities a day. Nothing is converted, cross-checked or blanked
here; the project's delivery layer does that.

**Retention:** `permanent_archive`, as far as observed: on 2026-11-27 `?year=2015` returned 2,256 records and
`?year=2000` an empty list. Older dates can be queried directly, so the source is not in
`config/windowed_sources.json` and a missed run loses nothing. The copy is kept because the portal's licence page says
"Data providers do not guarantee the continuity of data", and the API this one replaced
(`openapi.izmir.bel.tr/api/izsu/barajdurum`) answers HTTP 500 since November 2026.

**Schedule:** daily, 16:23 Europe/Istanbul (13:23 UTC). The first run walks back year by year until three years in a
row are empty and stores each year's answer once; a year whose request fails is asked for again by the next run.
Later runs ask for the current year (and the previous one in January), then for each new or changed date on its own.
Requests are made one at a time, 3 s apart: sixteen concurrent requests on 2026-11-27 got HTTP 502 for two of them.
Run the workflow by hand with `reread_all_years` to ask for the stored years again.

**Storage:** answers are stored as served and never overwritten: `raw/years/<YYYY>_<capturedUTC>__<hash12>.json.gz`
for a year, `raw/<YYYY>/<YYYYMMDD>_<capturedUTC>__<hash12>.json` for a single date; `raw/captures.csv` lists them.
`timeseries/turkey_izsu_<YYYY>.csv` holds the latest version of each date and
`timeseries/turkey_izsu_revisions.csv` every version of the dates whose values changed, both rebuilt from the stored
answers on every run. `metadata/turkey_izsu_reservoirs.csv` has one row per facility, with coordinates and capacities
from the dataset's own CSV (`raw/static/`) where that file names the facility. HTTP 403/412/429 stops the run.

**Licence:** CC BY 4.0 (`open_attribution`). The portal's licence page (<https://acikveri.bizizmir.com/en/license>,
checked 2026-11-27) reads "All materials under this site are licensed by Izmir Metropolitan Municipality under CC BY
4.0 international license, unless otherwise stated", and the dataset's licence field names that page.
Attribution: "Source: İzmir Büyükşehir Belediyesi Açık Veri Portalı / İZSU, CC BY 4.0."

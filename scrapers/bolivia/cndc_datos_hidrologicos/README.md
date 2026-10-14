# bolivia/cndc_datos_hidrologicos - CNDC daily hydrological data (Bolivia)

**Source:** <https://www.cndc.bo/wp-json/cndc/v1/datos-hidrologicos?fecha=YYYY-MM-DD> - the JSON behind the daily
hydrological table on the website of CNDC (Comite Nacional de Despacho de Carga), Bolivia's power-system operator.

**What is captured:** for each date, per hydro system (Zongo, Taquesi, Corani, Misicuni, Miguilla, Kanata,
San Jacinto), every reservoir or pond (29 on 2026-10-14, e.g. Corani, Misicuni, San Jacinto, Zongo, Tiquimani Lago,
Chojlla Lago, Angostura Lago, Carabuco Lago): `caudal_m3s` (flow), `capacidad_hm3` (capacity), `vol_inicial_hm3`
and `vol_final_hm3` (volume at the start and at the end of the day), as published. CNDC does not say which values
are measured: a date's end-of-day volume differs from the next date's start-of-day volume (Corani 2026-10-12 end
53.67 hm3, 2026-10-13 start 53.83 hm3) and the current date's end-of-day volume is served before the day has passed,
so end-of-day volumes look like programmed values. Every version of a date is kept.

**Retention:** `rolling_window`. The endpoint serves the last 30 dates (`fechas_disponibles`: 2026-09-15 to
2026-10-14 on 2026-10-14); older dates return `{"sin_datos": true}`. CNDC's weekly statistics files (from 2002) keep
weekly storage and inflow, but these daily values are not published anywhere else.

**Schedule:** 02:25, 08:25, 14:25 and 20:25 America/La_Paz (06:25, 12:25, 18:25 and 00:25 UTC). Each run requests
every available date (about 30 requests, 2 s apart), so each date gets more than a hundred capture opportunities.

**Storage:** a response is stored (`raw/<year>/<YYYYMMDD>__<hash12>.json`) once per hash of its data part (`fecha`
and `sistemas`; the rolling date list is ignored). `timeseries/bolivia_cndc_datos_hidrologicos.csv` holds the
latest version of each date and `timeseries/bolivia_cndc_datos_hidrologicos_all_versions.csv` every version, both
rebuilt from the stored responses on every run. HTTP 403/412/429 stops the run.

**Licence:** the website footer reads "(c) 2026 CNDC - Comite Nacional de Despacho de Carga de Bolivia. Todos los
derechos reservados." and grants no reuse permission - `restricted_use` (checked 2026-10-14). Collected here by the
owner's decision of 2026-10-14; see `DATA_LICENCE_NOTICE.md`. Presence in this repository is not permission to
republish.

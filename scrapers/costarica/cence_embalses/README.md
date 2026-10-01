# costarica/cence_embalses - ICE/CENCE reservoir levels (Costa Rica)

**Source:** <https://apps.grupoice.com/CenceWeb/CenceBoletinIntraDiario.jsf?init=true> - "Boletin Intra-Diario" of the
Centro Nacional de Control de Energia (CENCE), operated by the Division Operacion y Control del Sistema Electrico
(DOCSE) of the Instituto Costarricense de Electricidad (ICE).

**What is captured:** the "Niveles de Embalse" panel. One chart per regulating reservoir (Cachi, Arenal, Pirris,
Reventazon) holds three daily series for every day of the current year - `Real` (observed level, m a.s.l.; CENCE's
monthly reports call it "Nivel Real 00:00 horas"), `Programado` (planned level) and `Programado Anual` (annual
planning curve) - plus a table with the latest hourly SCADA reading ("Datos tomados del SCADA cada 15 minutos").
Only `Real` is an observation; the planned series are archived as published and must not be read as observations.

**Retention:** `rolling_window`, year to date. The chart shows only the current year and nothing on the page selects
another year, so a year's values disappear when the chart moves to the next year; 31 December is visible for about one
day. Checked 2026-10-01: the value for that day was published by 01:00 local time. The Internet Archive holds 33
captures of the page (2020-09 to 2026-06, none in 2022) that restore most of 2020, 2021, 2023, 2024 and 2025; import
them with `IMPORT_DIR` / `IMPORT_ORIGIN=internet_archive`.

**Schedule:** 07:20, 15:20 and 23:20 America/Costa_Rica (13:20, 21:20, 05:20 UTC). Every run reads the whole year.

**Storage:** a page is stored (`raw/<year>/<capturedUTC>__<hash12>.html.gz`) only when its parsed daily chart data are
new; the hash ignores the page's per-request parts (time banner, JSF ViewState, chart ids carrying a timestamp). The
hourly table of every run is appended to `raw/hourly_readings.jsonl`. `timeseries/` is rebuilt from the stored pages on
every run: `costarica_cence_embalses_daily.csv` (days with an observed level; latest value per reservoir and
day), `costarica_cence_embalses_planned.csv` (the planned series, including future days, keyed `planned_for`),
`costarica_cence_embalses_revisions.csv` (values that changed between captures) and
`costarica_cence_embalses_hourly.csv`.

**Licence:** the page states "(c) ICE Todos los Derechos Reservados" and gives no reuse permission -
`restricted_use`. Collected here by the owner's decision of 2026-10-01 (same route as `argentina/cammesa_hidro`); see
`DATA_LICENCE_NOTICE.md`. Presence in this repository is not permission to republish.

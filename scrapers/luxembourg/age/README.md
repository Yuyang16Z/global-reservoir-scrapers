# Luxembourg AGE reservoir level

This scraper accumulates the official recent observations for AGE station 40,
`Barrage Esch-Sauer`, associated with `Lac de la Haute-Sure`.

## Why it is scheduled twice daily

The public AGE graph endpoint exposes only a rolling window of about five days. The
workflow runs at `04:17` and `16:17 UTC` every day. These times are approximately
`05:17/17:17` in Luxembourg winter time and `06:17/18:17` in summer time.

- The first run normally captures the previous complete Luxembourg calendar day.
- The second run recovers delayed source updates, transient failures, and revisions.
- The five-day overlap allows missed runs to be repaired without hourly polling.
- A non-round minute reduces GitHub Actions congestion at the top of the hour.

The workflow fails when the accumulated series is more than three local calendar days
behind, making source or scheduler problems visible in GitHub Actions.

## Measurement and quality rules

- Source: AGE/Inondations.lu station 40 graph API.
- Accepted source points must explicitly have `simulated=false`.
- The daily value is the arithmetic mean of all regular 15-minute observations in the
  `Europe/Luxembourg` calendar day.
- Complete days contain 96 samples normally, 92 at the spring DST transition, and 100
  at the autumn DST transition.
- Partial days, malformed points, simulated values, and unknown simulation status are
  excluded. No interpolation or gap filling is performed.
- `water_level_masl (m NN)` is an official water elevation above Luxembourg's stated
  `m NN` vertical reference; it is not a relative gauge-height proxy.

Every run merges the current rolling window into the existing CSV by date. Older rows
are never replaced by a shorter source window. Dates still visible to the source may be
revised when AGE republishes a changed value.

## Outputs

```text
data/luxembourg/age/
|-- metadata/
|   |-- luxembourg_age_reservoirs.csv
|   `-- luxembourg_age_quality_periods.csv
|-- timeseries/daily/LUX_AGE_40.csv
|-- raw/daily/station_40_window_YYYY-MM-DD.json.gz
`-- run_logs/YYYYMMDDTHHMMSSZ_summary.json
```

Only one compressed raw rolling-window response is retained per UTC retrieval date.
This preserves audit evidence without storing two large duplicate snapshots every day.
The repeated quality explanation is stored once in `luxembourg_age_quality_periods.csv`
instead of being repeated on every observation row.

## Run locally

```bash
OUTPUT_DIR=/tmp/luxembourg-age \
  python scrapers/luxembourg/age/luxembourg_age_scraper.py
```

## Official sources and licence

- Dataset and CC0 licence: https://data.public.lu/en/datasets/niveau-deau/
- Graph API: https://inondations.lu/api/station/graph-data/40
- Station page: https://inondations.lu/basins/sauer?lang=en&show-details=&station=40
- Station sheet: http://geoportail.eau.etat.lu/pdf/hydrometrie/FichesStations/40-Esch-Sure.pdf
- Full historical data request: https://eau.gouvernement.lu/fr/demarches/demande-de-donnees.html

The official water-level dataset is published under CC0 1.0 Universal. Attribution is
not a licence condition, but the repository retains AGE provenance and processing notes.
The separately requested historical archive must be checked against any terms supplied
with that archive before it is added.

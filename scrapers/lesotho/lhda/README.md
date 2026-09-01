# Lesotho - LHDA dam fill rates

Captures the fill percentage that the **Lesotho Highlands Development
Authority** publishes for Katse and Mohale on its homepage,
<https://www.lhda.org.ls/>.

## Why this source exists

The project's Lesotho delivery was built from South Africa's DWS weekly
bulletin, which used to carry Katse and Mohale because they feed the Lesotho
Highlands Water Project into South Africa. **DWS dropped the three foreign dams
from that bulletin on 2026-06-16** and has not restored them, freezing the
delivery at 2026-06-15. LHDA operates these dams and publishes their fill
percentage itself, so it is both the natural continuation and, for its own
dams, the more direct authority.

## Retention class and cadence

`current_snapshot` - the widget shows one figure per dam and is overwritten in
place, so a change that is not captured is lost. Twice daily, matching the
other snapshot sources in this repository.

## What it does and does not give

- **Fill percentage only.** LHDA publishes no volume and no capacity to derive
  one from, so `storage_mcm` is not available here.
- Each dam carries **its own printed "updated on" date**, which is the
  observation date; the fetch time is never used as one.
- Percentages above 100 are the operator's own figures (Mohale read 100.16% on
  2026-08-30) and are recorded as printed.
- The same widget shows construction-progress percentages for Phase II tunnels
  and bridges. Those are project milestones, not storage, and the parser
  accepts only the named dams.

## Outputs

```
data/lesotho/lhda/
  raw/lhda_home_<stamp>.html
  metadata/lesotho_lhda_reservoirs.csv
  timeseries/lesotho_lhda_timeseries.csv    merged, idempotent
  run_logs/                                 status=source_unavailable on outage
```

A run that fetches the page but parses no dam block exits non-zero: the widget
is the entire source, so a markup change must be loud rather than silently
recorded as a successful empty run.

## Licence

Undeclared - LHDA publishes the figures with no reuse statement. Recorded as
`undeclared_review` in `DATA_LICENCE_NOTICE.md`: internal research use only.

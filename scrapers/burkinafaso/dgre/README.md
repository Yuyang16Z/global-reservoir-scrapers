# Burkina Faso - DGRE decadal hydrological notes

Archives the "Note d'information hydrologique" decadal PDFs from
<https://dgre.gov.bf/> (Direction Generale des Ressources en Eau), discovered
through the site's `sitemap.xml`.

## Why archive a site that keeps its documents online

DGRE's previous site, `eauburkina.org`, is dead (HTTP 522). Its 2016-2021
bulletin archive survives only as Internet Archive captures: two bulletins are
gone entirely and 25 more were truncated by the crawler's 1 MiB limit and had
to be repaired. A "permanent" archive with one custodian is one outage away
from not being permanent, so this scraper keeps an independent copy of the
current site's decadal notes as they appear.

## Scope

Decadal notes only. The monthly "Bulletin" PDFs (2-4 MB) are a prose product
without stored volumes and are not collected.

## Retention class and cadence

`permanent_archive` - published notes stay online, so nothing is lost between
runs. Per the owner's cadence rule for retrievable-history sources the
workflow runs on the 1st and 16th. It is deliberately NOT registered in
`config/windowed_sources.json`: the registry and its minimum
capture-opportunity audit exist for sources that lose unobserved data, and
the freshness monitor would misread DGRE's seasonal publishing (roughly
June-November, then months of silence) as staleness.

## Outputs

```
data/burkinafaso/dgre/
  raw/<name>.pdf     one file per note, identified by filename, never rewritten
  inventory.csv      filename, source URL, size, sha256, retrieval timestamp
  run_logs/          one JSON summary per run; status=source_unavailable on outage
```

## Licence

Undeclared - DGRE publishes the notes with no reuse statement. Recorded as
`undeclared_review` in `DATA_LICENCE_NOTICE.md`: internal research use only,
no redistribution without DGRE's permission.

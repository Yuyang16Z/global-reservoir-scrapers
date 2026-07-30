# Windowed and Ephemeral Source Archival Policy

This policy is mandatory for every source evaluated for the Global Reservoir
Dataset. It applies when an official source exposes only a rolling window,
current snapshot, replaceable bulletin, short-lived download, or recent table
whose older values disappear.

## Required discovery decision

Every source investigation must classify source retention as one of:

- `permanent_archive`: the official source provides a stable historical archive;
- `rolling_window`: observations disappear after a documented or observed period;
- `current_snapshot`: only the latest state is exposed;
- `overwrite_prone`: a stable URL is replaced by each new report or file;
- `unknown_review`: retention has not yet been established.

Record the evidence, observation date, source timezone, publication cadence,
estimated retention window, and whether older dates can be queried directly.
Do not assume a dashboard is archival merely because it accepts a date field.

## Mandatory deployment gate

A `rolling_window`, `current_snapshot`, or `overwrite_prone` source is not
considered operationally complete until it is registered in
`config/windowed_sources.json` and has an active archival deployment in
`Yuyang16Z/global-reservoir-scrapers`.

The deployment must include:

1. a source-specific scraper under `scrapers/<country>/<source>/`;
2. an automated GitHub Actions workflow under `.github/workflows/`, unless a
   documented external runner is technically required;
3. `workflow_dispatch` for recovery and backfill;
4. a schedule selected in the source's local time after its normal publication
   time, plus enough fallback opportunities to survive one missed run;
5. an overlap or lookback fetch that re-requests the available window instead
   of fetching only one presumed-new date;
6. idempotent merge keys, normally reservoir/station plus observation date and
   source series, so reruns do not duplicate observations;
7. immutable or checksum-traceable raw snapshots when the source may change;
8. persistent normalized snapshots or time series committed to the approved
   archive path;
9. retries with bounded backoff, a run summary, and explicit failure status;
10. concurrency protection and a timeout; and
11. a no-change exit that does not create empty commits.

GitHub cron is best-effort. The schedule must therefore leave enough margin for
delays and must not rely on one run immediately before a value expires.

## Frequency selection

Choose the schedule from evidence rather than applying one global cron:

- current or daily snapshots: normally twice per day, with runs separated and
  placed after likely source updates;
- two-day rolling tables: at least twice per day and re-fetch both days;
- five-to-eight-day windows: at least daily, preferably twice daily when the
  endpoint is unstable, and re-fetch the full available window;
- weekly replaceable reports: run after the normal publication time and again
  on a later day as a fallback;
- monthly replaceable reports: run after publication and retry on at least two
  later dates before the next issue.

The registry must state the timezone, cron, maximum expected schedule gap,
minimum capture opportunities, overlap strategy, and schedule rationale.

## Licence and publication gate

Automation does not override source terms. Before a public GitHub workflow
commits source data, record the licence, `reuse_status`, attribution, evidence
URL, and checked date.

- Open redistribution with required attribution may use the public repository
  when the attribution and source evidence are preserved.
- `undeclared_review`, `restricted_use`, `mixed_review`, or `prohibited` data
  must not be committed to a public repository unless the relevant portion has
  explicit permission. Use an approved private repository or storage system,
  or deploy metadata-only monitoring, until the rights are resolved.
- Credentials, session cookies, and private API keys must remain in GitHub
  Secrets and must never be written to raw snapshots or logs.

## Monitoring and recovery

Each deployment must expose enough evidence to detect silent failure:

- latest successful run timestamp;
- source dates seen and rows written;
- HTTP and parser failures;
- latest archived observation date;
- consecutive no-data or failed runs; and
- any source schema or URL change.

When the latest archived observation is older than the expected publication
cadence plus the documented grace period, treat the source as stale and repair
or manually rerun it before the retention window closes.

## Completion checklist

A new windowed source may be marked complete only when:

- the retention classification and evidence are documented;
- the registry entry is active;
- the scraper and workflow pass `scripts/audit_windowed_sources.py`;
- a manual workflow run has produced valid data and a run summary;
- a repeated run proves idempotency;
- recovery/backfill inputs have been tested; and
- licence and public-archive eligibility have been reviewed.

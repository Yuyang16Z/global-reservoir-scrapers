# Archive maintenance

Use this reference for a failed or stale scheduled archive, a green run with no new observations, an archival gap, or repository handoff.

## 1. Establish the active execution

Identify the source registry entry, workflow, scraper, output path, branch/revision deployed on GitHub, local worktree revision, latest committed data, and several recent run summaries. Keep these states separate.

Run the repository's windowed-source audit and freshness monitor when applicable. A passing audit checks registered entries; it is not a discovery scan proving every ephemeral source in the repository is registered.

## 2. Classify the condition

- **scraper defect:** official current data exists but fetch, parse, mapping, merge, or persistence fails;
- **upstream unavailable:** the official service cannot currently provide a valid response;
- **upstream stale:** the official source is reachable but has not published a new observation;
- **deployment defect:** schedule, permissions, checkout, concurrency, secret, or push behavior prevents archival persistence;
- **local-state mismatch:** code, registry, workflow, data, or Agent guidance came from different revisions;
- **source-contract change:** URL, schema, date semantics, object IDs, units, or terms changed.

Do not fabricate dates or return false success to make monitoring green. Preserve the last valid archive and report the real source state.

## 3. Repair and verify narrowly

Reproduce with saved raw evidence or an isolated output directory. Preserve source dates, raw evidence, unrelated user changes, and stable merge keys. Add a focused regression test for deterministic defects.

Verify a representative run, idempotent rerun, run summary, latest official observation date, row/object counts, registry audit, freshness behavior, and applicable tests. If the upstream remains unavailable or stale, report code correctness separately from real-world freshness.

External reruns, pushes, PRs, schedule changes, secrets, and new public exposure require authorization. Archive recovery does not prove formatted delivery; return to the parent workspace for that decision.

# personhog-backfill

Loads the person properties already in Postgres into SlateDB, once, before the writer switches to `dual`. It scans the person table in parallel key ranges and produces each row as a changelog-shaped record onto the backfill topic, which [personhog-writer](../personhog-writer/README.md) pods in `backfill` mode consume next to the live changelog. The store keeps the higher version per person, so a scanned row never overwrites a newer live one.

## Commands

`personhog-backfill plan --run-id <run>` splits `PG_TARGET_TABLE` into key ranges of about `--rows-per-range` rows (default 5M). Each child table of a partitioned table is split on its own, from a `TABLESAMPLE` of its keys, so no range spans two tables and nothing is counted. The plan is saved once under `<prefix>/maintenance/backfill/<run>/plan`; a run keeps the plan it started with.

`personhog-backfill scan --run-id <run> --index <n> --workers <count>` scans ranges `n`, `n + count`, and so on, in primary-key order, and produces each row to `PROPERTY_BACKFILL_TOPIC` on the changelog partition that person belongs to. Run it as an Indexed Job with `--index` set to the completion index and `--workers` to the completion count. A range records its cursor after every published page under `ranges/<number>/cursor`, so a restarted worker repeats at most one page, and a finished range (`ranges/<number>/done`) does nothing on rerun. A row whose properties do not parse is written to `rejects/<team>-<person>` and skipped.

The scan refuses to start while a changelog migration is draining, and when the backfill topic's partition count differs from the changelog's.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | The persons database to scan; use a clone, not the primary |
| `PG_TARGET_TABLE` | `personhog_person_tmp` | The person table (`posthog_person` in production) |
| `PROPERTY_BACKFILL_TOPIC` | (required) | The topic the scan writes; it must have the changelog's partition count |
| `KAFKA_HOSTS`, `KAFKA_TLS` | | The changelog cluster, as for the writer |
| `PROPERTY_STORE_URL`, `PROPERTY_STORE_PREFIX` | | Where the run's plan and progress files live, under the property store's prefix |
| `ETCD_ENDPOINTS`, `ETCD_PREFIX` | `http://localhost:2379`, `/personhog/` | Where the changelog config is read from |
| `METRICS_PORT` | `9102` | Prometheus endpoint of a `scan` worker |

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `personhog_backfill_rows_total{outcome}` | counter | Rows `read`, and of those `written` to the topic, `deleted` (written as delete markers) or `rejected` |
| `personhog_backfill_ranges_total{outcome}` | counter | Ranges this worker `finished` or found `already_done` |
| `personhog_backfill_page_seconds` | histogram | One page's Postgres scan |
| `personhog_backfill_produce_errors_total` | counter | Records the backfill topic refused |

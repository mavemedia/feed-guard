# feed-guard

Three small guards against silent failures in scheduled imports. No
dependencies, one file, roughly 180 lines.

These come out of a real incident: a nightly import ran with exit code 0 and a
clean log for eight days while writing exactly zero rows. The feed had changed
shape, every product hit a `continue` on the first line of the loop, and
nothing anywhere said so.

Full write-up:
[My pipeline ran green for 8 days while importing nothing](https://dev.to/mavemedia/my-pipeline-ran-green-for-8-days-while-importing-nothing-mec)

## The three guards

### 1. Count what you skip

Every `continue` in a parse loop is a silent discard. Count them by reason and
log the totals every run.

```python
from feed_guard import SkipCounter

skips = SkipCounter()
for product in feed:
    if product.get("country") not in NL_CODES:
        skips.skip("country not NL")
        continue
    write(product)
    skips.kept()

log.info(skips.report())
# seen=1887 kept=0 skipped=1887 (100%) | country not NL=1887

if skips.looks_broken():
    raise RuntimeError(skips.report())
```

### 2. Check freshness, not success

The single highest-value check in a scheduled import. Compare each source's
newest row against your cron interval.

```python
from datetime import timedelta
from feed_guard import check_freshness

stale = check_freshness(
    cursor,
    table="items",
    source_column="source",
    seen_column="last_seen",
    max_age=timedelta(days=2),
    where="active = 1",
)

for s in stale:
    log.error("stale source: %s", s)
# stale source: Landal: last seen 2025-09-08 13:21, 194h ago, 1126 rows
```

### 3. Refuse bulk changes that are too large to be real

Real inventory decays a few rows at a time. A fifth of a source vanishing
overnight is a parser symptom, not an inventory one.

```python
from feed_guard import guard_bulk_change, BulkChangeRefused

for source, gone in disappeared.items():
    try:
        guard_bulk_change(total=totals[source], affected=len(gone), label=source)
    except BulkChangeRefused as exc:
        log.error("%s", exc)
        continue
    deactivate(gone)
```

A cleanup job written a week earlier would have deactivated all 1126 rows
during that outage, correctly by its own logic, because they were absent from
every run.

## Install

Copy `feed_guard.py` into your project. That is the whole thing.

## Notes

* `check_freshness` takes any DB-API cursor. Identifiers are validated; the
  `where` clause is interpolated as written, so keep it developer-controlled.
* Timestamps are compared naively. Store and pass times in one zone.
* Ordering matters as much as thresholds: run cleanup after every fetcher and
  before anything that publishes.

## License

MIT

---

Built while running [Parkzie](https://parkzie.com), a Dutch holiday park
comparison site, by [Mave Media](https://mave-media.nl).

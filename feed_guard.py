"""
feed_guard: three small guards against silent failures in scheduled imports.

Written after a pipeline ran green for eight days while importing nothing:
https://dev.to/mavemedia/my-pipeline-ran-green-for-8-days-while-importing-nothing-mec

Exit code 0 means the script finished, not that it worked. These three guards
cover the gap:

  SkipCounter        count what a parse loop throws away, and why
  check_freshness    find sources whose newest row is older than it should be
  guard_bulk_change  refuse a destructive change that is too large to be real

No dependencies. check_freshness takes any DB-API cursor.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta

__all__ = [
    "SkipCounter",
    "StaleSource",
    "check_freshness",
    "BulkChangeRefused",
    "guard_bulk_change",
]

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str) -> str:
    """Reject anything that is not a plain identifier before it reaches SQL."""
    if not _IDENT.match(name):
        raise ValueError(f"not a plain SQL identifier: {name!r}")
    return name


class SkipCounter:
    """Count what a parse loop discards, grouped by reason.

    Every `continue` in a parse loop is a silent discard. Counted and logged,
    a feed that changes shape shows up as a number instead of as silence.

        skips = SkipCounter()
        for product in feed:
            if product.get("country") not in NL_CODES:
                skips.skip("country not NL")
                continue
            if not product.get("id"):
                skips.skip("no id")
                continue
            write(product)
            skips.kept()

        log.info(skips.report())
        if skips.looks_broken():
            raise RuntimeError(f"parser discarded almost everything: {skips.report()}")
    """

    def __init__(self) -> None:
        self._reasons: Counter = Counter()
        self._kept = 0

    def skip(self, reason: str, n: int = 1) -> None:
        self._reasons[reason] += n

    def kept(self, n: int = 1) -> None:
        self._kept += n

    @property
    def total_kept(self) -> int:
        return self._kept

    @property
    def total_skipped(self) -> int:
        return sum(self._reasons.values())

    @property
    def seen(self) -> int:
        return self._kept + self.total_skipped

    def reasons(self) -> dict:
        return dict(self._reasons.most_common())

    def skip_ratio(self) -> float:
        return self.total_skipped / self.seen if self.seen else 0.0

    def looks_broken(self, max_skip_ratio: float = 0.9) -> bool:
        """True when almost everything was discarded. Usually a parser bug."""
        return self.seen > 0 and self.skip_ratio() >= max_skip_ratio

    def report(self) -> str:
        if not self.seen:
            return "nothing seen"
        detail = " ".join(f"{r}={n}" for r, n in self._reasons.most_common())
        head = (
            f"seen={self.seen} kept={self._kept} "
            f"skipped={self.total_skipped} ({self.skip_ratio():.0%})"
        )
        return f"{head} | {detail}" if detail else head


@dataclass(frozen=True)
class StaleSource:
    source: str
    last_seen: datetime | None
    rows: int
    age: timedelta | None

    def __str__(self) -> str:
        if self.last_seen is None or self.age is None:
            return f"{self.source}: never seen, {self.rows} rows"
        hours = self.age.days * 24 + self.age.seconds // 3600
        return (
            f"{self.source}: last seen {self.last_seen:%Y-%m-%d %H:%M}, "
            f"{hours}h ago, {self.rows} rows"
        )


def check_freshness(
    cursor,
    *,
    table: str,
    source_column: str,
    seen_column: str,
    max_age: timedelta,
    where: str = "1=1",
    now: datetime | None = None,
) -> list[StaleSource]:
    """Return the sources whose newest row is older than `max_age`.

    This is the highest-value check in a scheduled import and it is four lines
    of SQL. Run it straight after the import and fail loudly on anything it
    returns. A source that is suddenly days behind the others has stopped
    importing, whatever the exit code said.

    `where` is interpolated as written, so keep it developer-controlled and
    never build it from user input. Identifiers are validated.

    Timestamps are compared naively, so store and pass times in one zone.
    """
    table = _ident(table)
    source_column = _ident(source_column)
    seen_column = _ident(seen_column)

    cursor.execute(
        f"SELECT {source_column} AS src, MAX({seen_column}) AS last_seen, COUNT(*) AS n "
        f"FROM {table} WHERE {where} GROUP BY {source_column}"
    )

    now = now or datetime.now()
    stale: list[StaleSource] = []

    for source, last_seen, n in cursor.fetchall():
        if last_seen is None:
            stale.append(StaleSource(str(source), None, int(n), None))
            continue
        age = now - last_seen
        if age > max_age:
            stale.append(StaleSource(str(source), last_seen, int(n), age))

    return sorted(stale, key=lambda s: s.age or timedelta.max, reverse=True)


class BulkChangeRefused(RuntimeError):
    """Raised when a bulk change is too large to be plausible."""


def guard_bulk_change(
    *,
    total: int,
    affected: int,
    max_pct: float = 20.0,
    max_abs: int = 50,
    label: str = "source",
) -> None:
    """Refuse a destructive bulk change that is too large to be real.

    Real inventory decays a few rows at a time. If a fifth of a source
    disappears overnight, what broke is upstream or in your own parser, and
    the right response is to refuse to act and say so.

    Call this before any mass deactivation or delete, once per source:

        for src, gone in disappeared.items():
            try:
                guard_bulk_change(total=totals[src], affected=len(gone), label=src)
            except BulkChangeRefused as exc:
                log.error("%s", exc)
                continue
            deactivate(gone)

    Ordering matters as much as the thresholds. Run the cleanup after every
    fetcher has finished and before anything that publishes, or it acts on
    yesterday's data or publishes rows it is about to retire.
    """
    if affected <= 0 or total <= 0:
        return

    pct = affected / total * 100

    if affected > max_abs:
        raise BulkChangeRefused(
            f"{label}: refusing to change {affected} rows at once, "
            f"the cap is {max_abs}. Check the parser before raising it."
        )

    if pct > max_pct:
        raise BulkChangeRefused(
            f"{label}: refusing to change {affected} of {total} rows "
            f"({pct:.0f}%), the cap is {max_pct:.0f}%. "
            f"Mass disappearance is a parser symptom, not an inventory one."
        )

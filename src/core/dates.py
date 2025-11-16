from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple


def iter_days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    """Yield every date between start and end (inclusive)."""
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += dt.timedelta(days=1)


def iter_date_windows(start: dt.date, end: dt.date, window_days: int) -> Iterable[Tuple[dt.date, dt.date]]:
    """Yield (start, end) pairs walking forward in ``window_days`` increments."""
    if window_days <= 0:
        window_days = 1
    current = start
    while current <= end:
        window_end = min(end, current + dt.timedelta(days=window_days - 1))
        yield current, window_end
        current = window_end + dt.timedelta(days=1)


def date_span_days(start: dt.date, end: dt.date) -> int:
    """Return the number of calendar days covered by the inclusive span."""
    return (end - start).days + 1


def group_missing_day_spans(
    day_range: Sequence[dt.date],
    location_dir: Path,
    *,
    include_today: bool = True,
    today: Optional[dt.date] = None,
) -> List[Tuple[dt.date, dt.date]]:
    """Group contiguous spans of missing CSVs for a location."""
    today = today or dt.date.today()
    spans: List[Tuple[dt.date, dt.date]] = []
    start: Optional[dt.date] = None
    end: Optional[dt.date] = None

    for day in day_range:
        cache_path = location_dir / f"{day.isoformat()}.csv"
        needs_refresh = (day == today and include_today) or (not cache_path.exists())
        if not needs_refresh:
            if start is not None:
                spans.append((start, end or start))
                start = end = None
            continue

        if start is None:
            start = end = day
        else:
            if end and (day - end == dt.timedelta(days=1)):
                end = day
            else:
                spans.append((start, end or start))
                start = end = day

    if start is not None:
        spans.append((start, end or start))
    return spans


def fetch_range_recursive(
    *,
    start: dt.date,
    end: dt.date,
    fetch_fn: Callable[[Dict[str, object]], object],
    request_kwargs: Dict[str, object],
    handle_payload: Callable[[object, dt.date, dt.date], Tuple[int, int, int]],
    progress,
) -> Tuple[int, int, int]:
    """
    Attempt a range request; on failure, split and recurse until single days.

    Returns (saved, skipped, errors).
    """
    try:
        progress.add_expected(1)
        payload = fetch_fn({**request_kwargs, "start": start, "end": end})
        progress.complete(1)
        return handle_payload(payload, start, end)
    except Exception as exc:  # pylint: disable=broad-except
        progress.complete(1)
        if date_span_days(start, end) <= 1:
            logging.warning("Range request failed for %s: %s", start, exc)
            return 0, 0, 1

        mid = start + dt.timedelta(days=date_span_days(start, end) // 2 - 1)
        left = (start, mid)
        right = (mid + dt.timedelta(days=1), end)
        s1, k1, e1 = fetch_range_recursive(
            start=left[0],
            end=left[1],
            fetch_fn=fetch_fn,
            request_kwargs=request_kwargs,
            handle_payload=handle_payload,
            progress=progress,
        )
        s2, k2, e2 = fetch_range_recursive(
            start=right[0],
            end=right[1],
            fetch_fn=fetch_fn,
            request_kwargs=request_kwargs,
            handle_payload=handle_payload,
            progress=progress,
        )
        return s1 + s2, k1 + k2, e1 + e2

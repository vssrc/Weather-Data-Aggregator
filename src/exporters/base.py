"""Base exporter class for all weather data providers."""

from __future__ import annotations

import datetime as dt
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Tuple

import pandas as pd


logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """
    Abstract base class for weather data exporters.

    Handles common export logic like:
    - Determining which dates need fetching
    - Managing file I/O
    - Error handling and logging
    - Counting saved/skipped/errors
    """

    def __init__(
        self,
        client: Any,
        provider_key: str,
        data_root: Path,
        locations: List[Tuple[str, float, float]],
        location_extras: dict,
    ):
        self.client = client
        self.provider_key = provider_key
        self.data_root = data_root
        self.locations = locations
        self.location_extras = location_extras
        self.provider_dir = data_root / provider_key
        self.provider_dir.mkdir(parents=True, exist_ok=True)

    def export(self, start_date: dt.date, end_date: dt.date) -> Tuple[int, int, int]:
        """
        Export data for all locations in the date range.

        Returns:
            Tuple of (saved, skipped, errors)
        """
        saved = skipped = errors = 0

        for location_key, lat, lon in self.locations:
            if not self.validate_location(location_key, lat, lon):
                logger.warning(f"{self.provider_key}: Skipping {location_key} (validation failed)")
                continue

            location_dir = self.provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            try:
                s, k, e = self.export_location(location_key, lat, lon, location_dir, start_date, end_date)
                saved += s
                skipped += k
                errors += e
            except Exception as exc:
                logger.exception(f"{self.provider_key}[{location_key}]: Unexpected error: {exc}")
                errors += 1

        return saved, skipped, errors

    def validate_location(self, location_key: str, lat: float, lon: float) -> bool:
        """
        Validate location before export. Override if provider needs specific validation.

        Returns:
            True if location is valid for this provider
        """
        return True

    @abstractmethod
    def export_location(
        self,
        location_key: str,
        lat: float,
        lon: float,
        location_dir: Path,
        start_date: dt.date,
        end_date: dt.date,
    ) -> Tuple[int, int, int]:
        """
        Export data for a single location.

        Must be implemented by subclass.

        Returns:
            Tuple of (saved, skipped, errors)
        """
        pass

    def group_missing_day_spans(
        self,
        location_dir: Path,
        start_date: dt.date,
        end_date: dt.date,
        include_today: bool = True,
    ) -> List[Tuple[dt.date, dt.date]]:
        """Find contiguous spans of missing CSV files."""
        spans: List[Tuple[dt.date, dt.date]] = []
        start: dt.date | None = None
        end_span: dt.date | None = None
        today = dt.date.today()

        current = start_date
        while current <= end_date:
            output_path = location_dir / f"{current.isoformat()}.csv"
            needs = (current == today and include_today) or (not output_path.exists())

            if not needs:
                if start is not None:
                    spans.append((start, end_span or start))
                    start = end_span = None
            else:
                if start is None:
                    start = end_span = current
                else:
                    if end_span and (current - end_span == dt.timedelta(days=1)):
                        end_span = current
                    else:
                        spans.append((start, end_span or start))
                        start = end_span = current

            current += dt.timedelta(days=1)

        if start is not None:
            spans.append((start, end_span or start))

        return spans

    def save_dataframe(
        self,
        df: pd.DataFrame,
        output_path: Path,
        location_key: str,
    ) -> bool:
        """
        Save DataFrame to CSV.

        Returns:
            True if saved successfully, False if skipped (empty)
        """
        if df.empty:
            return False

        df.to_csv(output_path, index=False)
        logger.info(f"{self.provider_key}[{location_key}]: Wrote {output_path}")
        return True

"""DataFrame-based exporter for providers that return pandas DataFrames."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

from .base import BaseExporter


logger = logging.getLogger(__name__)


class DataFrameExporter(BaseExporter):
    """
    Exporter for clients that return pandas DataFrames.

    Used by: Open-Meteo, NOAA ISD, NOAA LCD, NASA POWER, IEM ASOS
    """

    def __init__(self, *args, max_days_per_request: int = 30, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_days_per_request = max_days_per_request

    def export_location(
        self,
        location_key: str,
        lat: float,
        lon: float,
        location_dir: Path,
        start_date: dt.date,
        end_date: dt.date,
    ) -> Tuple[int, int, int]:
        """Export data for a single location using DataFrame-based client."""
        saved = skipped = errors = 0
        prefix = f"{self.provider_key}[{location_key}]"

        # Find missing date spans
        spans = self.group_missing_day_spans(location_dir, start_date, end_date, include_today=False)

        for span_start, span_end in spans:
            # Split into windows based on API limits
            current = span_start
            while current <= span_end:
                window_end = min(span_end, current + dt.timedelta(days=self.max_days_per_request - 1))

                try:
                    # Fetch data
                    location_param = self.get_location_param(location_key, lat, lon)
                    df = self.client.get_historical_data(
                        location=location_param,
                        start_date=current,
                        end_date=window_end,
                    )

                    # Process each day in the window
                    day = current
                    while day <= window_end:
                        output_path = location_dir / f"{day.isoformat()}.csv"

                        # Filter to specific day
                        if isinstance(df, pd.DataFrame) and not df.empty and "timestamp" in df.columns:
                            df_day = df[df["timestamp"].dt.date == day]

                            if df_day.empty:
                                logger.debug(f"{prefix}: No data for {day}")
                                skipped += 1
                            else:
                                if self.save_dataframe(df_day, output_path, location_key):
                                    saved += 1
                        else:
                            logger.debug(f"{prefix}: No data for {day}")
                            skipped += 1

                        day += dt.timedelta(days=1)

                except Exception as exc:
                    logger.warning(f"{prefix}: Error fetching {current} to {window_end}: {exc}")
                    # Count each day in the failed window as an error
                    days_in_window = (window_end - current).days + 1
                    errors += days_in_window

                current = window_end + dt.timedelta(days=1)

        return saved, skipped, errors

    def get_location_param(self, location_key: str, lat: float, lon: float):
        """
        Get location parameter for API call.

        Override this if provider needs special location format.
        """
        return (lat, lon)

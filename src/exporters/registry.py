"""Provider exporter registry."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Tuple

import pandas as pd

from .dataframe_exporter import DataFrameExporter

if TYPE_CHECKING:
    from ..clients.base import WeatherClient


logger = logging.getLogger(__name__)


class NoaaExporter(DataFrameExporter):
    """Exporter for NOAA ISD/LCD with station-based locations."""

    def __init__(self, *args, station_attr: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.station_attr = station_attr

    def validate_location(self, location_key: str, lat: float, lon: float) -> bool:
        """Check if location has required station ID."""
        station_id = self.location_extras.get(location_key, {}).get(self.station_attr)
        if not station_id:
            logger.warning(f"{self.provider_key}: Missing {self.station_attr} for {location_key}")
            return False
        return True

    def get_location_param(self, location_key: str, lat: float, lon: float):
        """Return station ID as location."""
        return location_key


class IemAsosExporter(DataFrameExporter):
    """Exporter for IEM ASOS with station and network."""

    def validate_location(self, location_key: str, lat: float, lon: float) -> bool:
        """Check if location has station and network."""
        extras = self.location_extras.get(location_key, {})
        station = extras.get("iemStation")
        network = extras.get("iemNetwork")
        if not station or not network:
            logger.warning(f"{self.provider_key}: Missing iemStation/iemNetwork for {location_key}")
            return False
        return True

    def get_location_param(self, location_key: str, lat: float, lon: float):
        """Return dict with station and network."""
        extras = self.location_extras.get(location_key, {})
        return {
            "station": extras["iemStation"],
            "network": extras["iemNetwork"],
        }


class MeteostatExporter(DataFrameExporter):
    """
    Exporter for Meteostat which returns List[dict] instead of DataFrame.

    Converts to DataFrame for processing.
    """

    def export_location(
        self,
        location_key: str,
        lat: float,
        lon: float,
        location_dir: Path,
        start_date: dt.date,
        end_date: dt.date,
    ) -> Tuple[int, int, int]:
        """Export data, handling List[dict] return type."""
        saved = skipped = errors = 0
        prefix = f"{self.provider_key}[{location_key}]"

        # Find missing date spans
        spans = self.group_missing_day_spans(location_dir, start_date, end_date, include_today=False)

        for span_start, span_end in spans:
            current = span_start
            while current <= span_end:
                window_end = min(span_end, current + dt.timedelta(days=self.max_days_per_request - 1))

                try:
                    # Fetch data - returns List[dict]
                    data = self.client.get_historical_data(
                        location=(lat, lon),
                        start_date=current,
                        end_date=window_end,
                    )

                    # Convert to DataFrame
                    if data:
                        df = pd.DataFrame(data)
                        if "timestamp" not in df.columns and "time" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["time"])
                        elif "timestamp" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["timestamp"])
                    else:
                        df = pd.DataFrame()

                    # Process each day
                    day = current
                    while day <= window_end:
                        output_path = location_dir / f"{day.isoformat()}.csv"

                        if not df.empty and "timestamp" in df.columns:
                            df_day = df[df["timestamp"].dt.date == day]
                            if df_day.empty:
                                skipped += 1
                            else:
                                if self.save_dataframe(df_day, output_path, location_key):
                                    saved += 1
                        else:
                            skipped += 1

                        day += dt.timedelta(days=1)

                except Exception as exc:
                    logger.warning(f"{prefix}: Error fetching {current} to {window_end}: {exc}")
                    days_in_window = (window_end - current).days + 1
                    errors += days_in_window

                current = window_end + dt.timedelta(days=1)

        return saved, skipped, errors


def create_exporter(
    client: WeatherClient,
    provider_key: str,
    data_root: Path,
    locations: List[Tuple[str, float, float]],
    location_extras: Dict,
    config: Dict,
) -> DataFrameExporter:
    """
    Factory function to create appropriate exporter for a provider.

    Args:
        client: Provider client instance
        provider_key: Provider identifier
        data_root: Root directory for data storage
        locations: List of (name, lat, lon) tuples
        location_extras: Extra location metadata
        config: Full configuration dict

    Returns:
        Configured exporter instance
    """
    # Get maxDaysPerRequest from config
    provider_cfg = config.get("providers", {}).get(provider_key, {})
    max_days = provider_cfg.get("maxDaysPerRequest", 30)

    if provider_key == "open_meteo":
        return DataFrameExporter(client, provider_key, data_root, locations, location_extras, max_days_per_request=max_days)

    elif provider_key == "noaa_isd":
        return NoaaExporter(client, provider_key, data_root, locations, location_extras, max_days_per_request=max_days, station_attr="noaaIsdStation")

    elif provider_key == "noaa_lcd":
        return NoaaExporter(client, provider_key, data_root, locations, location_extras, max_days_per_request=max_days, station_attr="noaaLcdStation")

    elif provider_key == "meteostat":
        return MeteostatExporter(client, provider_key, data_root, locations, location_extras, max_days_per_request=max_days)

    elif provider_key == "nasa_power":
        return DataFrameExporter(client, provider_key, data_root, locations, location_extras, max_days_per_request=max_days)

    elif provider_key == "iem_asos":
        return IemAsosExporter(client, provider_key, data_root, locations, location_extras, max_days_per_request=max_days)

    else:
        raise ValueError(f"Unknown provider: {provider_key}")

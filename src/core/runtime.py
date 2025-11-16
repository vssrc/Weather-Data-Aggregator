from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import load_locations, load_project_config, provider_setting
from .dates import iter_days


@dataclass
class ExportRuntime:
    """Holds derived runtime state shared across provider exporters."""

    config_path: Path
    start_date: dt.date
    end_date: dt.date
    config_data: dict = field(init=False, default_factory=dict)
    location_items: List[Tuple[str, float, float]] = field(init=False, default_factory=list)
    location_extras: Dict[str, Dict[str, object]] = field(init=False, default_factory=dict)
    day_range: List[dt.date] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.reload_config()
        self._refresh_day_range()
        self.reload_locations()

    def reload_config(self) -> None:
        self.config_data = load_project_config(self.config_path)

    def reload_locations(self, limit: Optional[int] = None) -> None:
        if not self.config_data:
            self.reload_config()
        locations, extras = load_locations(self.config_data)
        if limit is not None and limit > 0:
            limited = locations[:limit]
            allowed = {name for name, *_ in limited}
            extras = {name: extras.get(name, {}) for name in allowed}
            locations = limited
        self.location_items = locations
        self.location_extras = extras

    def update_dates(self, since: Optional[str], until: Optional[str]) -> None:
        if since:
            self.start_date = dt.date.fromisoformat(since)
        if until:
            self.end_date = dt.date.fromisoformat(until)
        if self.end_date < self.start_date:
            raise ValueError("--until must be on/after --since")
        self._refresh_day_range()

    def _refresh_day_range(self) -> None:
        self.day_range = list(iter_days(self.start_date, self.end_date))

    def provider_setting(self, provider_key: str, setting: str, default=None):
        return provider_setting(self.config_data, provider_key, setting, default)

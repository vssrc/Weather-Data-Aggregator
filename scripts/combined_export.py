#!/usr/bin/env python3
"""
Combined weather + GIBS export runner with unified terminal UI.

Providers:
- Weather: Open-Meteo, NOAA ISD/LCD, Meteostat
- Imagery: NASA GIBS layers (PNG)

Dates can be configured in config.json under 'date_range' with 'start' and 'end'.
Use 'today' as the end date to automatically use the current date.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients import (
    OpenMeteoClient,
    NoaaIsdClient,
    NoaaLcdClient,
    MeteostatClient,
    GibsClient,
)
from src.exporters import create_exporter, ImageExporter
from src.core import iter_days


# Paths and config defaults
CONFIG_PATH = Path("config.json")
DATA_ROOT = Path("data")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "combined_export.log"

# Weather defaults
START_DATE = dt.date.fromisoformat("2000-01-01")
END_DATE = dt.date.today()
COOLDOWN_SECONDS = 300
BATCH_SIZE = 30
MAX_WORKERS_PROVIDERS = 6
MAX_WORKERS_PER_PROVIDER = 10

# GIBS defaults
DEFAULT_GIBS_FREQ = "1d"
MAX_WORKERS_GIBS = 128
GIBS_REFRESH_DAYS = 1  # always refresh last N days
GIBS_WIDTH = 1024
GIBS_HEIGHT = 1024

# Provider toggles (can be overridden via --providers filter)
USE_OPEN_METEO = True
USE_NOAA_ISD = True
USE_NOAA_LCD = True
USE_METEOSTAT = True
USE_GIBS = True

# Ensure directories exist
DATA_ROOT.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Reset log on start
if LOG_FILE.exists():
    LOG_FILE.unlink()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
    force=True,
)
logger = logging.getLogger(__name__)


# ANSI color codes
class Colors:
    GREEN = "\033[92m"
    ORANGE = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"


class LogTail:
    """Thread-safe log tail tracker."""

    def __init__(self, max_lines: int = 12):
        self.max_lines = max_lines
        self.lines = deque(maxlen=max_lines)
        self.lock = Lock()

    def add(self, line: str) -> None:
        with self.lock:
            timestamp = dt.datetime.now().strftime("%H:%M:%S")
            self.lines.append(f"[{timestamp}] {line}")

    def get_lines(self) -> List[str]:
        with self.lock:
            return list(self.lines)


class ProgressTracker:
    """Thread-safe progress tracker that supports provider-specific step lists."""

    def __init__(
        self,
        providers: List[str],
        locations: List[Tuple[str, float, float]],
        *,
        steps_by_provider: Dict[str, List[object]],
        start: Union[dt.date, dt.datetime],
        end: Union[dt.date, dt.datetime],
    ):
        self.providers = providers
        self.locations = [loc[0] for loc in locations]
        self.steps_by_provider = steps_by_provider
        self.start = start
        self.end = end
        self.lock = Lock()
        self.log_tail = LogTail(max_lines=12)
        self.last_display_time = 0.0

        self.state: Dict[str, Dict[str, Dict[object, str]]] = {
            provider: {location: {} for location in self.locations} for provider in providers
        }
        self.stats: Dict[str, Dict[str, Dict[str, int]]] = {
            provider: {
                location: {
                    "cached": 0,
                    "failed": 0,
                    "processing": 0,
                    "total": len(steps_by_provider.get(provider, [])),
                }
                for location in self.locations
            }
            for provider in providers
        }
        self.current_ranges: Dict[str, Dict[str, Tuple[dt.date, dt.date]]] = {
            provider: {} for provider in providers
        }

    def steps_for(self, provider: str) -> List[object]:
        return self.steps_by_provider.get(provider, [])

    def set_status(self, provider: str, location: str, step: object, status: str) -> None:
        with self.lock:
            if provider not in self.state or location not in self.state[provider]:
                return
            old_status = self.state[provider][location].get(step)
            self.state[provider][location][step] = status

            if old_status == status:
                return
            if old_status == "processing":
                self.stats[provider][location]["processing"] -= 1

            if status == "cached":
                if old_status != "cached":
                    self.stats[provider][location]["cached"] += 1
            elif status == "failed":
                if old_status != "failed":
                    self.stats[provider][location]["failed"] += 1
            elif status == "processing":
                self.stats[provider][location]["processing"] += 1

    def set_current_range(self, provider: str, location: str, start: dt.date, end: dt.date) -> None:
        with self.lock:
            self.current_ranges.setdefault(provider, {})[location] = (start, end)

    def clear_current_range(self, provider: str, location: str) -> None:
        with self.lock:
            if provider in self.current_ranges and location in self.current_ranges[provider]:
                del self.current_ranges[provider][location]

    def log(self, message: str) -> None:
        self.log_tail.add(message)

    @staticmethod
    def _format_range(start: Union[dt.date, dt.datetime], end: Union[dt.date, dt.datetime]) -> str:
        if isinstance(start, dt.datetime):
            start_str = start.strftime("%Y-%m-%d %H:%M")
        else:
            start_str = start.isoformat()
        if isinstance(end, dt.datetime):
            end_str = end.strftime("%Y-%m-%d %H:%M")
        else:
            end_str = end.isoformat()
        return f"{start_str} to {end_str}"

    def _draw_progress_bar(self, cached: int, processing: int, failed: int, total: int, width: int = 30) -> str:
        if total == 0:
            return "[" + " " * width + "] 0%"

        cached_pct = cached / total if total else 0
        processing_pct = processing / total if total else 0
        failed_pct = failed / total if total else 0

        green_filled = int(width * cached_pct)
        orange_filled = int(width * processing_pct)
        red_filled = int(width * failed_pct)
        remaining = width - green_filled - orange_filled - red_filled

        bar = (
            f"{Colors.GREEN}{'=' * green_filled}{Colors.RESET}"
            f"{Colors.ORANGE}{'=' * orange_filled}{Colors.RESET}"
            f"{Colors.RED}{'=' * red_filled}{Colors.RESET}"
            f"{Colors.DIM}{'-' * remaining}{Colors.RESET}"
        )
        pct = int((cached / total) * 100) if total else 0
        return f"[{bar}] {pct:3d}% ({cached}/{total})"

    def display(self, force: bool = False) -> None:
        current_time = time.time()
        if not force and (current_time - self.last_display_time) < 1.0:
            return
        self.last_display_time = current_time

        print("\033[2J\033[H", end="", flush=True)
        with self.lock:
            print(f"\n{Colors.BOLD}Export Progress{Colors.RESET}")
            print(f"Range: {self._format_range(self.start, self.end)}")
            print("=" * 90)

            for provider in self.providers:
                print(f"\n{Colors.BOLD}{Colors.BLUE}{provider.upper()}{Colors.RESET}")
                print("-" * 90)
                print(f"{'Location':<20} {'Progress':<50} {'Current Range':<20}")

                for location in self.locations:
                    stats = self.stats[provider][location]
                    progress_bar = self._draw_progress_bar(
                        stats["cached"], stats["processing"], stats["failed"], stats["total"], width=30
                    )
                    current_range = ""
                    if location in self.current_ranges.get(provider, {}):
                        start, end = self.current_ranges[provider][location]
                        current_range = f"{Colors.ORANGE}>{start}..{end}{Colors.RESET}"
                    print(f"{location[:18]:<20} {progress_bar:<50} {current_range:<20}")

                total_cached = sum(self.stats[provider][loc]["cached"] for loc in self.locations)
                total_failed = sum(self.stats[provider][loc]["failed"] for loc in self.locations)
                total_all = sum(self.stats[provider][loc]["total"] for loc in self.locations)
                summary = f"  Summary: {total_cached:,} cached"
                if total_failed:
                    summary += f", {Colors.RED}{total_failed:,} failed{Colors.RESET}"
                summary += f" / {total_all:,} total"
                print(f"{Colors.DIM}{summary}{Colors.RESET}")

        print("\n" + "=" * 90)
        print(f"{Colors.BOLD}Recent Activity:{Colors.RESET}")
        print("-" * 90)
        log_lines = self.log_tail.get_lines()
        if log_lines:
            for line in log_lines:
                print(f"{Colors.DIM}{line}{Colors.RESET}")
        else:
            print(f"{Colors.DIM}(No activity yet){Colors.RESET}")
        print("=" * 90)
        print(f"{Colors.DIM}Log: {LOG_FILE} | Green=Cached Orange=Processing Red=Failed{Colors.RESET}")


def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Config file not found: %s", CONFIG_PATH)
        print(f"Error: Config file not found: {CONFIG_PATH}")
        sys.exit(1)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in config file: %s", exc)
        print(f"Error: Invalid JSON in config file: {exc}")
        sys.exit(1)


def load_locations(config: Mapping[str, object]) -> Tuple[List[Tuple[str, float, float]], Dict[str, Dict[str, object]]]:
    locations_dict = config.get("locations", {})
    if not isinstance(locations_dict, Mapping):
        logger.error("The configuration must define a 'locations' mapping.")
        sys.exit(1)

    location_items: List[Tuple[str, float, float]] = []
    location_extras: Dict[str, Dict[str, object]] = {}
    for name, coords in locations_dict.items():
        try:
            lat = float(coords["lat"])
            lon = float(coords["lon"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.error("Invalid coordinates for location '%s': %s", name, exc)
            continue
        location_items.append((name, lat, lon))
        extras = {k: v for k, v in coords.items() if k not in {"lat", "lon"}}
        location_extras[name] = extras

    if not location_items:
        logger.error("No valid locations found in config")
        print("Error: No valid locations found in config")
        sys.exit(1)
    return location_items, location_extras


def parse_frequency(token: str) -> dt.timedelta:
    token = token.strip().lower()
    if token.endswith("min"):
        return dt.timedelta(minutes=int(token[:-3]))
    if token.endswith("m"):
        return dt.timedelta(minutes=int(token[:-1]))
    if token.endswith("h"):
        return dt.timedelta(hours=int(token[:-1]))
    if token.endswith("d"):
        return dt.timedelta(days=int(token[:-1]))
    raise ValueError("Frequency must end with m/min, h, or d (e.g. 30m, 1h, 1d)")


def generate_timestamps(start: dt.datetime, end: dt.datetime, freq: dt.timedelta) -> List[dt.datetime]:
    ts = []
    cursor = start
    while cursor <= end:
        ts.append(cursor)
        cursor += freq
    return ts


def parse_date_arg(value: Optional[str], *, default: dt.date) -> dt.date:
    if not value:
        return default
    return dt.date.fromisoformat(value)


def parse_datetime_arg(value: Optional[str], *, default: dt.datetime) -> dt.datetime:
    if not value:
        return default
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def parse_config_date(value: str) -> dt.date:
    """
    Parse a date from config, supporting 'today' keyword.
    
    Args:
        value: Either 'today' or a date string in YYYY-MM-DD format.
        
    Returns:
        The parsed date, or today's date if value is 'today'.
    """
    if value.lower() == "today":
        return dt.date.today()
    return dt.date.fromisoformat(value)


def check_existing_weather_cache(
    tracker: ProgressTracker,
    provider: str,
    location_key: str,
    location_dir: Path,
    steps: Iterable[dt.date],
    refresh_recent: bool = True,
) -> None:
    """Check for cached dates in the consolidated CSV file."""
    import pandas as pd
    yesterday = dt.date.today() - dt.timedelta(days=1)
    today = dt.date.today()
    
    csv_file = location_dir / f"{location_key}.csv"
    cached_dates = set()
    
    if csv_file.exists():
        try:
            df = pd.read_csv(csv_file)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                cached_dates = set(df['timestamp'].dt.date.unique())
        except Exception:
            pass  # If file is corrupted, treat as no cache
    
    for step in steps:
        if refresh_recent and step in (yesterday, today):
            continue
        if step in cached_dates:
            tracker.set_status(provider, location_key, step, "cached")
    
    # Log summary of initial cache check
    cached_count = sum(1 for step in steps if tracker.state[provider][location_key].get(step) == "cached")
    if cached_count > 0:
        tracker.log(f"{provider}/{location_key}: Found {cached_count} cached dates in {location_key}.csv")


def check_existing_gibs_cache(
    tracker: ProgressTracker,
    provider: str,
    location_key: str,
    base_dir: Path,
    layer: str,
    timestamps: Iterable[dt.datetime],
    should_refresh_fn,
) -> int:
    """
    Pre-mark cached GIBS images so we skip scheduling downloads.

    Returns:
        Number of cached images detected.
    """
    cached = 0
    for ts in timestamps:
        filename = ts.strftime("%Y-%m-%dT%H%M%SZ") + ".png"
        path = base_dir / filename
        if path.exists() and not should_refresh_fn(ts):
            step = (layer, ts)
            tracker.set_status(provider, location_key, step, "cached")
            cached += 1

    if cached:
        tracker.log(f"{provider}/{location_key}/{layer}: Found {cached} cached images")
    return cached


def process_date_batch(
    client,
    exporter,
    provider_key: str,
    location_key: str,
    location_param,
    batch: List[dt.date],
    location_dir: Path,
    tracker: ProgressTracker,
) -> Tuple[List[pd.DataFrame], int]:
    """Fetch data for a batch of dates and return DataFrames (not save yet)."""
    import pandas as pd
    
    dfs = []
    errors = 0
    for day in batch:
        try:
            tracker.set_status(provider_key, location_key, day, "processing")
            tracker.display()

            df = client.get_historical_data(location=location_param, start_date=day, end_date=day)

            # Normalize list-of-dicts (Meteostat) to DataFrame
            if isinstance(df, list):
                df = pd.DataFrame(df)
            has_data = isinstance(df, pd.DataFrame) and not df.empty

            if has_data:
                dfs.append(df)
                tracker.set_status(provider_key, location_key, day, "cached")
            else:
                tracker.set_status(provider_key, location_key, day, "failed")
                errors += 1
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("%s[%s] %s: %s", provider_key, location_key, day, exc)
            tracker.set_status(provider_key, location_key, day, "failed")
            tracker.log(f"{provider_key}/{location_key} {day}: Error - {str(exc)[:50]}")
            errors += 1
    return dfs, errors


def export_location_parallel(
    provider_key: str,
    client,
    exporter,
    location_key: str,
    lat: float,
    lon: float,
    location_dir: Path,
    steps_to_fetch: List[dt.date],
    tracker: ProgressTracker,
) -> Tuple[int, int]:
    """Fetch dates for a location and save incrementally to CSV after each batch."""
    import pandas as pd
    from threading import Lock
    
    if not steps_to_fetch:
        return 0, 0

    csv_path = location_dir / f"{location_key}.csv"
    csv_lock = Lock()  # Thread-safe CSV writes
    total_saved = 0
    errors = 0
    location_param = exporter.get_location_param(location_key, lat, lon)
    batches = [steps_to_fetch[i : i + BATCH_SIZE] for i in range(0, len(steps_to_fetch), BATCH_SIZE)]
    tracker.log(f"{provider_key}/{location_key}: Processing {len(steps_to_fetch)} dates in {len(batches)} batches")

    def append_to_csv(new_dfs: List[pd.DataFrame]) -> int:
        """Thread-safe append of new data to CSV, returns row count saved."""
        if not new_dfs:
            return 0
        
        with csv_lock:
            new_data = pd.concat(new_dfs, ignore_index=True)
            if 'timestamp' in new_data.columns:
                new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])
            
            if csv_path.exists():
                try:
                    existing = pd.read_csv(csv_path)
                    if 'timestamp' in existing.columns:
                        existing['timestamp'] = pd.to_datetime(existing['timestamp'])
                    combined = pd.concat([existing, new_data], ignore_index=True)
                    if 'timestamp' in combined.columns:
                        combined = combined.drop_duplicates(subset=['timestamp'], keep='last')
                        combined = combined.sort_values('timestamp')
                    combined.to_csv(csv_path, index=False)
                    return len(new_data)
                except Exception as exc:
                    logger.warning(f"{provider_key}/{location_key}: Merge error, appending fresh: {exc}")
                    new_data.to_csv(csv_path, index=False)
                    return len(new_data)
            else:
                if 'timestamp' in new_data.columns:
                    new_data = new_data.sort_values('timestamp')
                new_data.to_csv(csv_path, index=False)
                return len(new_data)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_PROVIDER) as executor:
        futures = {
            executor.submit(
                process_date_batch,
                client,
                exporter,
                provider_key,
                location_key,
                location_param,
                batch,
                location_dir,
                tracker,
            ): batch
            for batch in batches
        }
        for future in as_completed(futures):
            batch = futures[future]
            try:
                batch_dfs, batch_errors = future.result()
                errors += batch_errors
                
                # Save incrementally after each batch
                if batch_dfs:
                    rows_saved = append_to_csv(batch_dfs)
                    total_saved += len(batch_dfs)
                    tracker.log(f"{provider_key}/{location_key}: Saved batch ({rows_saved} rows)")
                
                tracker.display()
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("%s[%s] batch error: %s", provider_key, location_key, exc)
                errors += len(batch)
    
    if total_saved > 0:
        tracker.log(f"{provider_key}/{location_key}: Total {total_saved} batches saved to {location_key}.csv")
    
    return total_saved, errors




def export_weather_provider(
    provider_key: str,
    client_class,
    enabled: bool,
    config: dict,
    locations: List[Tuple[str, float, float]],
    location_extras: Dict[str, Dict[str, object]],
    steps: List[dt.date],
    tracker: ProgressTracker,
) -> str:
    if not enabled:
        tracker.log(f"{provider_key}: Disabled")
        return f"{provider_key}: disabled"
    try:
        tracker.log(f"{provider_key}: Initializing...")
        client = client_class(config_path=CONFIG_PATH)
        exporter = create_exporter(
            client=client,
            provider_key=provider_key,
            data_root=DATA_ROOT,
            locations=locations,
            location_extras=location_extras,
            config=config,
        )

        tracker.log(f"{provider_key}: Checking cache...")
        for location_name, _, _ in locations:
            # Check cache at provider directory level (no location subdirectory)
            if not tracker.state[provider_key][location_name]:
                check_existing_weather_cache(
                    tracker, provider_key, location_name, exporter.provider_dir, steps, refresh_recent=True
                )

        tracker.display()
        total_saved = 0
        total_errors = 0

        for location_key, lat, lon in locations:
            if not exporter.validate_location(location_key, lat, lon):
                tracker.log(f"{provider_key}/{location_key}: Skipping (validation failed)")
                continue

            # Use provider directory directly (no location subdirectory)
            provider_dir = exporter.provider_dir
            provider_dir.mkdir(parents=True, exist_ok=True)

            to_fetch = [d for d in steps if tracker.state[provider_key][location_key].get(d) != "cached"]
            if not to_fetch:
                tracker.log(f"{provider_key}/{location_key}: All dates cached")
                continue

            saved, errors = export_location_parallel(
                provider_key, client, exporter, location_key, lat, lon, provider_dir, to_fetch, tracker
            )
            total_saved += saved
            total_errors += errors
            tracker.log(f"{provider_key}/{location_key}: {saved} saved, {errors} errors")
            tracker.display()

        summary = f"{provider_key}: {total_saved} saved, {total_errors} errors"
        tracker.log(f"{provider_key}: Complete - {total_saved} saved, {total_errors} errors")
        logger.info(summary)
        return summary
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("%s: Failed: %s", provider_key, exc)
        tracker.log(f"{provider_key}: FAILED - {str(exc)[:80]}")
        # Mark all remaining steps as failed so UI reflects the crash
        for location_key, _, _ in locations:
            for step in steps:
                if tracker.state[provider_key][location_key].get(step) != "cached":
                    tracker.set_status(provider_key, location_key, step, "failed")
        tracker.display(force=True)
        return f"{provider_key}: failed"


def export_gibs(
    client: GibsClient,
    *,
    layers: Sequence[str],
    locations: List[Tuple[str, float, float]],
    location_extras: Dict[str, Dict[str, object]],
    timestamps: List[dt.datetime],
    tracker: ProgressTracker,
    refresh_days: int,
) -> str:
    provider_key = "gibs"
    tracker.log(f"{provider_key}: Initializing...")

    def should_refresh(ts: dt.datetime) -> bool:
        if refresh_days <= 0:
            return False
        cutoff = dt.datetime.combine(dt.date.today(), dt.time(0, 0)) - dt.timedelta(days=refresh_days - 1)
        return ts >= cutoff

    total_saved = 0
    total_errors = 0

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_GIBS) as executor:
        for location_key, _, _ in locations:
            extras = location_extras.get(location_key, {})
            bbox = extras.get("bbox")
            if not bbox:
                tracker.log(f"{provider_key}/{location_key}: Missing bbox, skipping")
                continue
            for layer in layers:
                base_dir = DATA_ROOT / provider_key / layer / location_key
                base_dir.mkdir(parents=True, exist_ok=True)
                check_existing_gibs_cache(
                    tracker,
                    provider_key,
                    location_key,
                    base_dir,
                    layer,
                    timestamps,
                    should_refresh,
                )
                for ts in timestamps:
                    step = (layer, ts)
                    filename = ts.strftime("%Y-%m-%dT%H%M%SZ") + ".png"
                    path = base_dir / filename
                    if path.exists() and not should_refresh(ts):
                        continue
                    tasks.append(
                        executor.submit(
                            fetch_gibs_image,
                            client,
                            provider_key,
                            layer,
                            bbox,
                            ts,
                            filename,
                            base_dir,
                            location_key,
                            step,
                            tracker,
                        )
                    )

        for future in as_completed(tasks):
            saved, errors = future.result()
            total_saved += saved
            total_errors += errors
            tracker.display()

    tracker.log(f"{provider_key}: Complete - {total_saved} saved, {total_errors} errors")
    logger.info("%s: %s saved, %s errors", provider_key, total_saved, total_errors)
    return f"{provider_key}: {total_saved} saved, {total_errors} errors"


def fetch_gibs_image(
    client: GibsClient,
    provider_key: str,
    layer: str,
    bbox: Sequence[float],
    timestamp: dt.datetime,
    filename: str,
    base_dir: Path,
    location_key: str,
    step: object,
    tracker: ProgressTracker,
) -> Tuple[int, int]:
    saved = errors = 0
    try:
        tracker.set_status(provider_key, location_key, step, "processing")
        tracker.display()

        image_bytes, _ = client.get_image(
            bbox=bbox,
            timestamp=timestamp,
            layer=layer,
            width=GIBS_WIDTH,
            height=GIBS_HEIGHT,
        )
        exporter = ImageExporter(base_dir)
        exporter.save(filename, image_bytes)
        tracker.set_status(provider_key, location_key, step, "cached")
        saved += 1
    except Exception as exc:  # pylint: disable=broad-except
        tracker.set_status(provider_key, location_key, step, "failed")
        tracker.log(f"{provider_key}/{location_key}/{layer} {timestamp}: {str(exc)[:80]}")
        logger.exception("%s/%s/%s %s", provider_key, location_key, layer, timestamp, exc)
        errors += 1
    return saved, errors


def run_export_once(
    config: dict,
    locations: List[Tuple[str, float, float]],
    location_extras: Dict[str, Dict[str, object]],
    weather_steps: List[dt.date],
    gibs_steps: List[dt.datetime],
    provider_filter: Optional[set],
    gibs_layers: Sequence[str],
    gibs_refresh_days: int,
) -> None:
    providers = {
        "open_meteo": (OpenMeteoClient, USE_OPEN_METEO),
        "noaa_isd": (NoaaIsdClient, USE_NOAA_ISD),
        "noaa_lcd": (NoaaLcdClient, USE_NOAA_LCD),
        "meteostat": (MeteostatClient, USE_METEOSTAT),
        "gibs": (GibsClient, USE_GIBS),
    }
    if provider_filter:
        providers = {k: v for k, v in providers.items() if k in provider_filter}
    enabled_providers = [k for k, (_, enabled) in providers.items() if enabled]
    if not enabled_providers:
        print("Warning: No providers enabled")
        return

    gibs_combined_steps = [(layer, ts) for layer in gibs_layers for ts in gibs_steps]

    steps_by_provider: Dict[str, List[object]] = {}
    for key in enabled_providers:
        if key == "gibs":
            steps_by_provider[key] = gibs_combined_steps
        else:
            steps_by_provider[key] = weather_steps

    weather_start_dt = dt.datetime.combine(weather_steps[0], dt.time(0, 0))
    weather_end_dt = dt.datetime.combine(weather_steps[-1], dt.time(23, 59))
    timeline_start = weather_start_dt
    timeline_end = weather_end_dt
    if gibs_steps:
        timeline_start = min(timeline_start, gibs_steps[0])
        timeline_end = max(timeline_end, gibs_steps[-1])

    tracker = ProgressTracker(
        enabled_providers,
        locations,
        steps_by_provider=steps_by_provider,
        start=timeline_start,
        end=timeline_end,
    )
    tracker.log(
        f"Export started: {len(enabled_providers)} providers, {len(locations)} locations, "
        f"{len(weather_steps)} weather days, {len(gibs_combined_steps)} gibs steps"
    )

    # Pre-populate cached statuses so the UI and work planner skip existing files up front
    for provider in enabled_providers:
        if provider == "gibs":
            continue
        provider_dir = DATA_ROOT / provider
        provider_dir.mkdir(parents=True, exist_ok=True)
        for location_key, _, _ in locations:
            check_existing_weather_cache(
                tracker, provider, location_key, provider_dir, weather_steps, refresh_recent=True
            )

    tracker.display()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PROVIDERS) as executor:
        futures = {}
        for provider_key, (client_class, enabled) in providers.items():
            if not enabled:
                continue
            if provider_key == "gibs":
                gibs_cfg = config.get("providers", {}).get("gibs", {})
                api_key = gibs_cfg.get("token")
                client = GibsClient(api_key=api_key)
                futures[executor.submit(
                    export_gibs,
                    client,
                    layers=gibs_layers,
                    locations=locations,
                    location_extras=location_extras,
                    timestamps=gibs_steps,
                    tracker=tracker,
                    refresh_days=gibs_refresh_days,
                )] = provider_key
            else:
                futures[executor.submit(
                    export_weather_provider,
                    provider_key,
                    client_class,
                    enabled,
                    config,
                    locations,
                    location_extras,
                    weather_steps,
                    tracker,
                )] = provider_key

        for future in as_completed(futures):
            provider_key = futures[future]
            try:
                result = future.result()
                logger.info("Completed: %s", result)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("%s: Unexpected error: %s", provider_key, exc)
                tracker.log(f"{provider_key}: CRASHED - {str(exc)[:80]}")

    tracker.display(force=True)
    print(f"\n{Colors.BOLD}{Colors.GREEN}Export complete.{Colors.RESET} Check {LOG_FILE} for details.\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Combined weather + GIBS export runner")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument("--since", type=str, default=None, help="Override start date YYYY-MM-DD (from config if not set)")
    parser.add_argument("--until", type=str, default=None, help="Override end date YYYY-MM-DD or 'today' (from config if not set)")
    parser.add_argument("--providers", type=str, default=None, help="Comma-separated provider keys")
    parser.add_argument("--limit-locations", type=int, default=None, help="Limit number of locations")
    parser.add_argument("--gibs-frequency", type=str, default=None, help="Override frequency for GIBS (e.g. 1d, 6h, 30m)")
    parser.add_argument("--gibs-refresh-days", type=int, default=GIBS_REFRESH_DAYS, help="Refresh window for GIBS (days)")
    args = parser.parse_args()

    config = load_config()
    locations, location_extras = load_locations(config)
    if args.limit_locations and args.limit_locations > 0:
        locations = locations[: args.limit_locations]
        logger.info("Limited to %s location(s)", len(locations))

    # Read dates from config, with CLI override
    date_range_cfg = config.get("date_range", {})
    config_start = date_range_cfg.get("start", "2000-01-01")
    config_end = date_range_cfg.get("end", "today")
    
    if args.since:
        start_date = parse_config_date(args.since)
    else:
        start_date = parse_config_date(config_start)
    
    if args.until:
        end_date = parse_config_date(args.until)
    else:
        end_date = parse_config_date(config_end)
    
    if end_date < start_date:
        logger.error("End date must be on or after start date")
        print("Error: End date must be on or after start date")
        sys.exit(1)
    
    logger.info("Date range: %s to %s", start_date, end_date)

    # Read GIBS frequency from config, with CLI override
    gibs_cfg = config.get("providers", {}).get("gibs", {})
    gibs_freq_str = args.gibs_frequency or gibs_cfg.get("frequency", DEFAULT_GIBS_FREQ)
    freq = parse_frequency(gibs_freq_str)
    gibs_start_dt = dt.datetime.combine(start_date, dt.time(0, 0))
    gibs_end_dt = dt.datetime.combine(end_date, dt.time(23, 59))
    gibs_steps = generate_timestamps(gibs_start_dt, gibs_end_dt, freq)

    weather_steps = list(iter_days(start_date, end_date))

    provider_filter = None
    if args.providers:
        provider_filter = {p.strip() for p in args.providers.split(",") if p.strip()}

    gibs_layers = config.get("providers", {}).get("gibs", {}).get("layers", [])
    if not gibs_layers:
        logger.info("No GIBS layers configured; GIBS provider will be effectively empty.")

    try:
        if args.once:
            run_start = time.time()
            run_export_once(
                config,
                locations,
                location_extras,
                weather_steps,
                gibs_steps,
                provider_filter,
                gibs_layers,
                args.gibs_refresh_days,
            )
            elapsed = time.time() - run_start
            logger.info("Export completed in %.2f seconds", elapsed)
            return

        while True:
            run_start = time.time()
            try:
                run_export_once(
                    config,
                    locations,
                    location_extras,
                    weather_steps,
                    gibs_steps,
                    provider_filter,
                    gibs_layers,
                    args.gibs_refresh_days,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Error during export run: %s", exc)
            elapsed = time.time() - run_start
            logger.info("Export completed in %.2f seconds", elapsed)
            logger.info("Sleeping for %s seconds", COOLDOWN_SECONDS)
            print(f"\nSleeping for {COOLDOWN_SECONDS // 60} minutes...")
            try:
                time.sleep(COOLDOWN_SECONDS)
            except KeyboardInterrupt:
                logger.info("Interrupted during sleep, exiting")
                raise
    except KeyboardInterrupt:
        logger.info("Export interrupted, shutting down")
        print("\nShutdown complete.")


if __name__ == "__main__":
    main()

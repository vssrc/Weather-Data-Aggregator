#!/usr/bin/env python3
"""
Weather data export runner with visual progress display.

Fetches weather data from multiple providers and caches to CSV files.
Features:
- Parallel provider execution
- Batched parallel requests within providers
- Visual progress with date range indicators
- Smart caching (skip cached days except yesterday/today)
- Live log tail
"""

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
from typing import Dict, List, Tuple, Set

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients import (
    OpenMeteoClient,
    NoaaIsdClient,
    NoaaLcdClient,
    MeteostatClient,
    NasaPowerClient,
    IemAsosClient,
)
from src.exporters import create_exporter
from src.core import iter_days


# Configuration
CONFIG_PATH = Path("config.json")
DATA_ROOT = Path("data")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "weather_export.log"

# Default date range: last 7 days ending yesterday
START_DATE = dt.date.fromisoformat("2000-01-01")
END_DATE = dt.date.today()
COOLDOWN_SECONDS = 300  # 5 minutes between cycles

# Provider toggles
USE_OPEN_METEO = True
USE_NOAA_ISD = True
USE_NOAA_LCD = True
USE_METEOSTAT = True
USE_NASA_POWER = True
USE_IEM_ASOS = True

# Performance settings
MAX_WORKERS_PROVIDERS = 6  # Parallel providers
MAX_WORKERS_PER_PROVIDER = 10  # Parallel date batches per provider
BATCH_SIZE = 30  # Days per request batch

# Ensure directories exist
DATA_ROOT.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Clear log file at startup
if LOG_FILE.exists():
    LOG_FILE.unlink()

# Configure logging - file only, no console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
    force=True,
)

logger = logging.getLogger(__name__)


# ANSI color codes
class Colors:
    GREEN = '\033[92m'
    ORANGE = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'


class LogTail:
    """Thread-safe log tail tracker."""

    def __init__(self, max_lines=10):
        self.max_lines = max_lines
        self.lines = deque(maxlen=max_lines)
        self.lock = Lock()

    def add(self, line: str):
        """Add a log line."""
        with self.lock:
            timestamp = dt.datetime.now().strftime("%H:%M:%S")
            self.lines.append(f"[{timestamp}] {line}")

    def get_lines(self) -> List[str]:
        """Get current log lines."""
        with self.lock:
            return list(self.lines)


class ProgressTracker:
    """Thread-safe progress tracker with visual date range display."""

    def __init__(self, providers: List[str], locations: List[Tuple[str, float, float]],
                 start_date: dt.date, end_date: dt.date):
        self.providers = providers
        self.locations = [loc[0] for loc in locations]
        self.dates = list(iter_days(start_date, end_date))
        self.start_date = start_date
        self.end_date = end_date
        self.total_days = len(self.dates)

        # State: provider -> location -> date -> status
        self.state: Dict[str, Dict[str, Dict[dt.date, str]]] = {
            provider: {
                location: {} for location in self.locations
            } for provider in providers
        }

        # Stats: provider -> location -> {cached, failed, processing, total}
        self.stats: Dict[str, Dict[str, Dict[str, int]]] = {
            provider: {
                location: {'cached': 0, 'failed': 0, 'processing': 0, 'total': self.total_days}
                for location in self.locations
            } for provider in providers
        }

        # Current date ranges being processed: provider -> location -> (start, end)
        self.current_ranges: Dict[str, Dict[str, Tuple[dt.date, dt.date]]] = {
            provider: {} for provider in providers
        }

        self.lock = Lock()
        self.log_tail = LogTail(max_lines=12)
        self.last_display_time = 0.0  # Throttle display updates

    def set_status(self, provider: str, location: str, date: dt.date, status: str):
        """Update status for a specific provider/location/date."""
        with self.lock:
            if provider in self.state and location in self.state[provider]:
                old_status = self.state[provider][location].get(date)
                self.state[provider][location][date] = status

                # Update stats
                if old_status != status:
                    # Decrement old status
                    if old_status == 'processing':
                        self.stats[provider][location]['processing'] -= 1

                    # Increment new status
                    if status == 'cached':
                        if old_status not in ('cached',):
                            self.stats[provider][location]['cached'] += 1
                    elif status == 'failed':
                        if old_status not in ('failed',):
                            self.stats[provider][location]['failed'] += 1
                    elif status == 'processing':
                        self.stats[provider][location]['processing'] += 1

    def set_current_range(self, provider: str, location: str, start: dt.date, end: dt.date):
        """Set the current date range being processed."""
        with self.lock:
            if provider in self.current_ranges:
                self.current_ranges[provider][location] = (start, end)

    def clear_current_range(self, provider: str, location: str):
        """Clear the current date range."""
        with self.lock:
            if provider in self.current_ranges and location in self.current_ranges[provider]:
                del self.current_ranges[provider][location]

    def check_existing_cache(self, provider: str, location: str, refresh_recent: bool = True):
        """
        Check which dates are already cached.

        Args:
            refresh_recent: If True, always re-fetch yesterday and today
        """
        provider_dir = DATA_ROOT / provider / location
        if not provider_dir.exists():
            return

        yesterday = dt.date.today() - dt.timedelta(days=1)
        today = dt.date.today()

        with self.lock:
            for date in self.dates:
                # Skip caching for yesterday/today if refresh_recent is True
                if refresh_recent and date in (yesterday, today):
                    continue

                csv_file = provider_dir / f"{date.isoformat()}.csv"
                if csv_file.exists():
                    self.state[provider][location][date] = 'cached'
                    self.stats[provider][location]['cached'] += 1

    def log(self, message: str):
        """Add a log message."""
        self.log_tail.add(message)

    def _draw_progress_bar(self, cached: int, processing: int, failed: int, total: int, width: int = 30) -> str:
        """Draw a progress bar with green/orange/red sections."""
        if total == 0:
            return '[' + ' ' * width + '] 0%'

        # Calculate fills
        cached_pct = cached / total if total > 0 else 0
        processing_pct = processing / total if total > 0 else 0
        failed_pct = failed / total if total > 0 else 0

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

        pct = int((cached / total) * 100) if total > 0 else 0
        return f"[{bar}] {pct:3d}% ({cached}/{total})"

    def _format_date_range(self, start: dt.date, end: dt.date) -> str:
        """Format a date range compactly."""
        if start == end:
            return f"{start.strftime('%Y-%m-%d')}"
        elif start.year == end.year and start.month == end.month:
            return f"{start.strftime('%Y-%m')}-{start.day:02d}..{end.day:02d}"
        elif start.year == end.year:
            return f"{start.strftime('%Y')}-{start.strftime('%m-%d')}..{end.strftime('%m-%d')}"
        else:
            return f"{start.strftime('%y-%m-%d')}..{end.strftime('%y-%m-%d')}"

    def display(self, force: bool = False):
        """Display the progress grid with stats and log tail."""
        # Throttle updates to once per second unless forced
        current_time = time.time()
        if not force and (current_time - self.last_display_time) < 1.0:
            return

        self.last_display_time = current_time

        # Clear screen and move to top
        print('\033[2J\033[H', end='', flush=True)

        with self.lock:
            # Header
            print(f"\n{Colors.BOLD}Weather Data Export Progress{Colors.RESET}")
            print(f"Range: {self.start_date} to {self.end_date} ({self.total_days:,} days)")
            print("=" * 90)

            for provider in self.providers:
                # Provider header
                print(f"\n{Colors.BOLD}{Colors.BLUE}{provider.upper()}{Colors.RESET}")
                print("-" * 90)

                # Table header
                print(f"{'Location':<20} {'Progress':<50} {'Current Range':<20}")

                # Rows (locations)
                for location in self.locations:
                    stats = self.stats[provider][location]
                    cached = stats['cached']
                    processing = stats['processing']
                    failed = stats['failed']
                    total = stats['total']

                    # Progress bar
                    progress_bar = self._draw_progress_bar(cached, processing, failed, total, width=30)

                    # Current range being processed
                    current_range = ""
                    if location in self.current_ranges.get(provider, {}):
                        start, end = self.current_ranges[provider][location]
                        current_range = f"{Colors.ORANGE}>{self._format_date_range(start, end)}{Colors.RESET}"

                    print(f"{location[:18]:<20} {progress_bar:<50} {current_range:<20}")

                # Provider summary
                total_cached = sum(self.stats[provider][loc]['cached'] for loc in self.locations)
                total_failed = sum(self.stats[provider][loc]['failed'] for loc in self.locations)
                total_all = sum(self.stats[provider][loc]['total'] for loc in self.locations)

                summary = f"  Summary: {total_cached:,} cached"
                if total_failed > 0:
                    summary += f", {Colors.RED}{total_failed:,} failed{Colors.RESET}"
                summary += f" / {total_all:,} total"
                print(f"{Colors.DIM}{summary}{Colors.RESET}")

        # Log tail at bottom
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


def load_config():
    """Load configuration file."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {CONFIG_PATH}")
        print(f"Error: Config file not found: {CONFIG_PATH}")
        sys.exit(1)
    except json.JSONDecodeError as exc:
        logger.error(f"Invalid JSON in config file: {exc}")
        print(f"Error: Invalid JSON in config file: {exc}")
        sys.exit(1)


def load_locations(config):
    """Extract locations from config."""
    locations_dict = config.get("locations", {})
    location_items = []
    location_extras = {}

    for name, coords in locations_dict.items():
        try:
            lat = float(coords["lat"])
            lon = float(coords["lon"])
            location_items.append((name, lat, lon))

            extras = {k: v for k, v in coords.items() if k not in {"lat", "lon"}}
            location_extras[name] = extras
        except (KeyError, TypeError, ValueError) as exc:
            logger.error(f"Invalid coordinates for location '{name}': {exc}")
            continue

    if not location_items:
        logger.error("No valid locations found in config")
        print("Error: No valid locations found in config")
        sys.exit(1)

    return location_items, location_extras


def process_date_batch(client, exporter, provider_key, location_key, location_param,
                       batch: List[dt.date], location_dir: Path, tracker: ProgressTracker) -> Tuple[int, int]:
    """
    Process a batch of dates.
    Fetches data day-by-day for the given batch.
    """
    saved = 0
    errors = 0

    for date in batch:
        try:
            # Mark as processing
            tracker.set_status(provider_key, location_key, date, 'processing')
            tracker.display()

            # Fetch data for single day
            import pandas as pd
            df = client.get_historical_data(
                location=location_param,
                start_date=date,
                end_date=date,
            )

            # Check if we got data
            has_data = False
            if isinstance(df, pd.DataFrame):
                has_data = not df.empty
            elif isinstance(df, list):
                has_data = len(df) > 0

            if has_data:
                # Save to file
                csv_path = location_dir / f"{date.isoformat()}.csv"
                if isinstance(df, pd.DataFrame):
                    df.to_csv(csv_path, index=False)
                else:
                    # Convert list of dicts to DataFrame
                    pd.DataFrame(df).to_csv(csv_path, index=False)

                tracker.set_status(provider_key, location_key, date, 'cached')
                saved += 1
            else:
                tracker.set_status(provider_key, location_key, date, 'failed')
                errors += 1

        except Exception as exc:
            logger.exception(f"{provider_key}[{location_key}] {date}: {exc}")
            tracker.set_status(provider_key, location_key, date, 'failed')
            tracker.log(f"{provider_key}/{location_key} {date}: Error - {str(exc)[:50]}")
            errors += 1

    return saved, errors


def export_location_parallel(provider_key, client, exporter, location_key, lat, lon,
                             location_dir: Path, dates_to_fetch: List[dt.date],
                             tracker: ProgressTracker):
    """
    Export data for a single location using parallel batched requests.
    """
    if not dates_to_fetch:
        return 0, 0

    saved = 0
    errors = 0

    location_param = exporter.get_location_param(location_key, lat, lon)

    # Split dates into batches
    batches = [dates_to_fetch[i:i + BATCH_SIZE] for i in range(0, len(dates_to_fetch), BATCH_SIZE)]
    tracker.log(f"{provider_key}/{location_key}: Processing {len(dates_to_fetch)} dates in {len(batches)} batches")

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_PROVIDER) as executor:
        futures = {
            executor.submit(
                process_date_batch,
                client, exporter, provider_key, location_key, location_param,
                batch, location_dir, tracker
            ): batch
            for batch in batches
        }

        for future in as_completed(futures):
            batch = futures[future]
            try:
                batch_saved, batch_errors = future.result()
                saved += batch_saved
                errors += batch_errors
                tracker.display()
            except Exception as exc:
                logger.exception(f"{provider_key}[{location_key}] batch error: {exc}")
                errors += len(batch)

    return saved, errors


def export_provider_with_tracking(provider_key, client_class, enabled, config, locations,
                                  location_extras, start_date, end_date, tracker: ProgressTracker):
    """Export data for a single provider with progress tracking."""
    if not enabled:
        logger.info(f"{provider_key}: Disabled")
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

        # Check existing cache (skip yesterday/today for refresh)
        tracker.log(f"{provider_key}: Checking cache...")
        for location_name, _, _ in locations:
            tracker.check_existing_cache(provider_key, location_name, refresh_recent=True)

        tracker.display()

        total_saved = 0
        total_errors = 0

        # Process each location
        for location_key, lat, lon in locations:
            if not exporter.validate_location(location_key, lat, lon):
                tracker.log(f"{provider_key}/{location_key}: Skipping (validation failed)")
                continue

            location_dir = exporter.provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            # Determine which dates need to be fetched
            dates_to_fetch = []
            for date in tracker.dates:
                status = tracker.state[provider_key][location_key].get(date)
                if status != 'cached':  # Fetch if not already cached
                    dates_to_fetch.append(date)

            if not dates_to_fetch:
                tracker.log(f"{provider_key}/{location_key}: All dates cached")
                continue

            # Export this location with parallel batched requests
            saved, errors = export_location_parallel(
                provider_key, client, exporter, location_key, lat, lon,
                location_dir, dates_to_fetch, tracker
            )

            total_saved += saved
            total_errors += errors

            tracker.log(f"{provider_key}/{location_key}: {saved} saved, {errors} errors")
            tracker.display()

        summary = f"{provider_key}: {total_saved} saved, {total_errors} errors"
        logger.info(summary)
        tracker.log(f"{provider_key}: Complete - {total_saved} saved, {total_errors} errors")
        return summary

    except Exception as exc:
        logger.exception(f"{provider_key}: Failed: {exc}")
        tracker.log(f"{provider_key}: FAILED - {str(exc)[:50]}")
        return f"{provider_key}: failed"


def run_export_once(config, locations, location_extras, start_date, end_date, provider_filter=None):
    """Run a single export cycle across all enabled providers (in parallel)."""
    providers = {
        "open_meteo": (OpenMeteoClient, USE_OPEN_METEO),
        "noaa_isd": (NoaaIsdClient, USE_NOAA_ISD),
        "noaa_lcd": (NoaaLcdClient, USE_NOAA_LCD),
        "meteostat": (MeteostatClient, USE_METEOSTAT),
        "nasa_power": (NasaPowerClient, USE_NASA_POWER),
        "iem_asos": (IemAsosClient, USE_IEM_ASOS),
    }

    if provider_filter:
        providers = {k: v for k, v in providers.items() if k in provider_filter}

    if not providers:
        logger.warning("No providers enabled")
        print("Warning: No providers enabled")
        return

    enabled_providers = [k for k, (_, enabled) in providers.items() if enabled]

    logger.info(f"Starting export for {len(enabled_providers)} provider(s) from {start_date} to {end_date}")

    tracker = ProgressTracker(enabled_providers, locations, start_date, end_date)
    tracker.log(f"Export started: {len(enabled_providers)} providers, {len(locations)} locations, {tracker.total_days:,} days")

    # Initial cache check and display
    for provider in enabled_providers:
        for location_name, _, _ in locations:
            tracker.check_existing_cache(provider, location_name, refresh_recent=True)

    tracker.display()

    # Run exports in parallel across providers
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PROVIDERS) as executor:
        futures = {
            executor.submit(
                export_provider_with_tracking,
                provider_key,
                client_class,
                enabled,
                config,
                locations,
                location_extras,
                start_date,
                end_date,
                tracker
            ): provider_key
            for provider_key, (client_class, enabled) in providers.items()
            if enabled
        }

        for future in as_completed(futures):
            provider_key = futures[future]
            try:
                result = future.result()
                logger.info(f"Completed: {result}")
            except Exception as exc:
                logger.exception(f"{provider_key}: Unexpected error: {exc}")
                tracker.log(f"{provider_key}: CRASHED - {str(exc)[:50]}")

    # Final display
    tracker.display(force=True)
    print(f"\n{Colors.BOLD}{Colors.GREEN}Export complete.{Colors.RESET} Check {LOG_FILE} for details.\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Weather data export runner")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument("--since", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--until", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--providers", type=str, default=None, help="Comma-separated provider keys")
    parser.add_argument("--limit-locations", type=int, default=None, help="Limit number of locations")

    args = parser.parse_args()

    config = load_config()
    locations, location_extras = load_locations(config)

    if args.limit_locations and args.limit_locations > 0:
        locations = locations[:args.limit_locations]
        logger.info(f"Limited to {len(locations)} location(s)")

    start_date = dt.date.fromisoformat(args.since) if args.since else START_DATE
    end_date = dt.date.fromisoformat(args.until) if args.until else END_DATE

    if end_date < start_date:
        logger.error("End date must be on or after start date")
        print("Error: End date must be on or after start date")
        sys.exit(1)

    provider_filter = None
    if args.providers:
        provider_filter = {p.strip() for p in args.providers.split(",") if p.strip()}

    logger.info("Weather export script initialized")

    try:
        if args.once:
            run_start = time.time()
            run_export_once(config, locations, location_extras, start_date, end_date, provider_filter)
            elapsed = time.time() - run_start
            logger.info(f"Export completed in {elapsed:.2f} seconds")
            return

        # Continuous mode
        while True:
            run_start = time.time()
            try:
                run_export_once(config, locations, location_extras, start_date, end_date, provider_filter)
            except Exception as exc:
                logger.exception(f"Error during export run: {exc}")

            elapsed = time.time() - run_start
            logger.info(f"Export completed in {elapsed:.2f} seconds")
            logger.info(f"Sleeping for {COOLDOWN_SECONDS} seconds")

            print(f"\nSleeping for {COOLDOWN_SECONDS // 60} minutes...")

            try:
                time.sleep(COOLDOWN_SECONDS)
            except KeyboardInterrupt:
                logger.info("Interrupted during sleep, exiting")
                raise

    except KeyboardInterrupt:
        logger.info("Weather export script interrupted, shutting down")
        print("\nShutdown complete.")


if __name__ == "__main__":
    main()

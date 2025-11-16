import datetime as dt
import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

def _clear_console() -> None:
    """Clears the console screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def generate_cached_coverage_chart(
    *,
    start_date: dt.date,
    end_date: dt.date,
    provider_keys: Sequence[str],
    provider_labels: Mapping[str, str],
    provider_resolution: Mapping[str, str],
    data_root: Path,
    locations: Iterable[Tuple[str, float, float]],
    in_progress_dates: Optional[Set[Tuple[str, str, dt.date]]] = None,
    buffered_logs: Optional[List[str]] = None,
) -> None:
    """Render a cache coverage heatmap for each provider/location."""
    _clear_console() # Clear console at the beginning of each redraw

    if in_progress_dates is None:
        in_progress_dates = set()
    if buffered_logs is None:
        buffered_logs = []

    date_range = [start_date + dt.timedelta(n) for n in range(int((end_date - start_date).days) + 1)]
    if not provider_keys:
        print("No providers configured for chart; skipping.") # Changed from logging.info to print
        return

    location_names = [name for name, *_ in locations]

    # ANSI escape codes for colors
    GREEN = "\033[92m"
    ORANGE = "\033[93m"  # Using yellow for orange as it's commonly supported
    RESET = "\033[0m"

    print(f"\nCached coverage {start_date.isoformat()} — {end_date.isoformat()}")
    print("-" * 80)

    for provider_key in provider_keys:
        label = provider_labels.get(provider_key, provider_key)
        resolution = provider_resolution.get(provider_key, "")
        title_suffix = f" ({resolution})" if resolution else ""
        print(f"\nProvider: {label}{title_suffix}")
        print("-" * (len(label) + len(title_suffix) + 10))

        provider_dir = data_root / provider_key

        if not location_names:
            print("  No location data available")
            continue

        # Print header row for dates
        date_headers = " ".join([day.strftime("%m/%d") for day in date_range])
        print(f"{'Location':<20} {date_headers}")

        for location_name in location_names:
            location_dir = provider_dir / location_name
            coverage_str = []
            for day in date_range:
                if (provider_key, location_name, day) in in_progress_dates:
                    coverage_str.append("X") # In progress
                elif (location_dir / f"{day.isoformat()}.csv").exists():
                    coverage_str.append("X")  # Cached
                else:
                    coverage_str.append(".")  # Blank for not cached/not requested

            print(f"{location_name:<20} {' '.join(coverage_str)}")
    print("-" * 80)
    # Print buffered logs after the chart
    if buffered_logs:
        print("\n--- Recent Logs ---")
        for log_msg in buffered_logs:
            print(log_msg)
        print("-------------------")
    # Removed logging.info("Displayed cached coverage in terminal.") as it will be handled by buffered logs


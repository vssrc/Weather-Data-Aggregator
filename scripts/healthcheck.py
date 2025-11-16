#!/usr/bin/env python3
"""Health check script for all weather providers."""

import datetime as dt
from pathlib import Path
import sys
import pandas as pd
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients import (
    OpenMeteoClient,
    NoaaIsdClient,
    NoaaLcdClient,
    MeteostatClient,
    NasaPowerClient,
    IemAsosClient,
)

CONFIG_PATH = Path('config.json')
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path('config.json.template')

# Load locations
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

locations_dict = config.get('locations', {})
LOCATION_ITEMS = [(name, coords['lat'], coords['lon']) for name, coords in locations_dict.items()]
LOCATION_EXTRAS = {name: {k: v for k, v in coords.items() if k not in {'lat', 'lon'}}
                   for name, coords in locations_dict.items()}

# Define providers
PROVIDERS = {
    'open_meteo': {'client': OpenMeteoClient},
    'noaa_isd': {'client': NoaaIsdClient},
    'noaa_lcd': {'client': NoaaLcdClient},
    'meteostat': {'client': MeteostatClient},
    'nasa_power': {'client': NasaPowerClient},
    'iem_asos': {'client': IemAsosClient},
}

HEALTHCHECK_START = dt.date.today() - dt.timedelta(days=730)
HEALTHCHECK_END = dt.date.today() - dt.timedelta(days=727)

def run_healthcheck(client_name, client_class):
    print(f'--- Healthcheck for: {client_name} ---')
    client = client_class(config_path=CONFIG_PATH)
    for name, lat, lon in LOCATION_ITEMS:
        location = f'{lat},{lon}'
        if client_name == 'noaa_isd':
            location = LOCATION_EXTRAS.get(name, {}).get('noaaIsdStation')
        elif client_name == 'noaa_lcd':
            location = LOCATION_EXTRAS.get(name, {}).get('noaaLcdStation')
        elif client_name == 'iem_asos':
            station = LOCATION_EXTRAS.get(name, {}).get('iemStation')
            network = LOCATION_EXTRAS.get(name, {}).get('iemNetwork')
            if not station or not network:
                print(f'Skipping {name} for {client_name} due to missing IEM station/network info.')
                continue
            location = {'station': station, 'network': network}

        if not location:
            print(f'Skipping {name} for {client_name} due to missing location info.')
            continue

        try:
            result = client.get_historical_data(
                location=location,
                start_date=HEALTHCHECK_START,
                end_date=HEALTHCHECK_END,
            )
            # Handle both DataFrame and List[dict] return types
            if isinstance(result, pd.DataFrame):
                if not result.empty:
                    print(f'Successfully fetched {len(result)} rows for {name}')
                    print(result.head())
                else:
                    print(f'No data returned for {name}')
            elif isinstance(result, list):
                if result:
                    print(f'Successfully fetched {len(result)} records for {name}')
                    print(result[:5])
                else:
                    print(f'No data returned for {name}')
            else:
                print(f'Unexpected return type for {name}: {type(result)}')
        except Exception as e:
            print(f'Error fetching data for {name}: {e}')
    print('='*50)

for provider_name, provider_info in PROVIDERS.items():
    if provider_info.get('enabled', True):
        run_healthcheck(provider_name, provider_info['client'])

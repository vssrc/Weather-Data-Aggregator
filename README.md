# Weather Data Aggregator

A clean, modular Python system for collecting, caching, and exploring weather data from multiple providers.

## Features

- **6 Weather Providers**: Open-Meteo, NOAA ISD, NOAA LCD, Meteostat, NASA POWER, IEM ASOS
- **Modular Architecture**: Clean separation of concerns with SOLID principles
- **Parallel Execution**: Concurrent provider requests for faster data collection
- **Smart Caching**: Daily CSV files with gap detection and filling
- **Type-Safe**: Comprehensive type hints throughout
- **Tested**: Verified with real-world data

## Project Structure

```
DL Project/
├── src/                          # Main source code
│   ├── clients/                  # API client implementations
│   │   ├── common.py            # Base classes & mixins
│   │   ├── config_loader.py     # Configuration utilities
│   │   ├── open_meteo_client.py # Open-Meteo client
│   │   ├── noaa_access_client.py # NOAA ISD/LCD client
│   │   ├── meteostat_client.py  # Meteostat client
│   │   ├── nasa_power_client.py # NASA POWER client
│   │   └── iem_asos_client.py   # IEM ASOS client
│   │
│   ├── exporters/               # Export logic abstraction
│   │   ├── base.py              # Abstract BaseExporter
│   │   ├── dataframe_exporter.py # DataFrame-based exporter
│   │   └── registry.py          # Provider factory & specializations
│   │
│   ├── core/                    # Core utilities
│   │   ├── config.py            # Config loading
│   │   ├── dates.py             # Date utilities
│   │   └── runtime.py           # Runtime dataclass
│   │
│   └── visualization/           # Visualization tools
│       └── coverage.py          # Coverage heatmap
│
├── scripts/                     # Executable scripts
│   ├── export.py               # Main export runner
│   └── healthcheck.py          # Provider health checker
│
├── tests/                       # Test suite
│   └── test_basic.py           # Basic import & functionality tests
│
├── data/                        # Cached CSV output (gitignored)
├── logs/                        # Export logs (gitignored)
├── config.json                  # User configuration (gitignored)
└── config.json.template         # Configuration template
```

## Quick Start

### 1. Installation

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configuration

```bash
cp config.json.template config.json
# Edit config.json with your NOAA token
```

Configuration format:
```json
{
  "providers": {
    "noaa_isd": {"token": "YOUR_TOKEN"},
    "noaa_lcd": {"token": "YOUR_TOKEN"}
  },
  "locations": {
    "new_york": {
      "lat": 40.78,
      "lon": -73.97,
      "noaaIsdStation": "72505394728",
      "noaaLcdStation": "72505394728",
      "iemStation": "NYC",
      "iemNetwork": "NY_ASOS"
    }
  }
}
```

### 3. Run Export

```bash
# Export last 7 days for all providers
python scripts/export.py --once

# Custom date range
python scripts/export.py --once --since 2024-11-01 --until 2024-11-07

# Specific providers only
python scripts/export.py --once --providers open_meteo,meteostat

# Limit locations for testing
python scripts/export.py --once --limit-locations 1

# Continuous mode (runs every 5 minutes)
python scripts/export.py
```

### 4. Health Check

```bash
python scripts/healthcheck.py
```

## Architecture

### Exporter Pattern

The architecture uses a **Template Method** pattern:

```python
# All exporters inherit from BaseExporter
class BaseExporter(ABC):
    def export(self, start_date, end_date):
        """Template method - same for all providers"""
        for location in self.locations:
            spans = self.group_missing_day_spans(location_dir)
            for span_start, span_end in spans:
                self.export_location(location, span_start, span_end)

    @abstractmethod
    def export_location(self, location, start, end):
        """Provider-specific implementation"""
        pass
```

**Benefits:**
- No code duplication
- Easy to add new providers
- Consistent behavior across providers
- Testable in isolation

### Provider Specializations

```python
# Most providers use the standard DataFrame exporter
DataFrameExporter  # Open-Meteo, NASA POWER

# Some need minor customization
NoaaExporter       # Adds station ID validation
IemAsosExporter    # Adds station + network validation
MeteostatExporter  # Converts List[dict] → DataFrame
```

## Testing

```bash
# Run basic tests
python tests/test_basic.py

# Test single provider
python scripts/export.py --once --providers open_meteo --limit-locations 1
```

## Supported Providers

| Provider | Type | Authentication | Notes |
|----------|------|----------------|-------|
| **Open-Meteo** | Free | None | Hourly forecast + historical archive |
| **NOAA ISD** | Free | Token required | Global hourly observations |
| **NOAA LCD** | Free | Token required | Local climatological data |
| **Meteostat** | Free | None | Multi-source hourly blend |
| **NASA POWER** | Free | None | Satellite/model hourly data |
| **IEM ASOS** | Free | None | 1-minute ASOS observations |

Get NOAA token: https://www.ncdc.noaa.gov/cdo-web/token

## Adding a Provider

1. **Create client** in `src/clients/your_provider.py`:
```python
class YourProviderClient(WeatherClient):
    def get_historical_data(self, location, start_date, end_date):
        # Fetch data from API
        return dataframe
```

2. **Add to registry** in `src/exporters/registry.py`:
```python
elif provider_key == "your_provider":
    return DataFrameExporter(...)
```

3. **Update main script** in `scripts/export.py`:
```python
from src.clients import YourProviderClient

providers = {
    "your_provider": (YourProviderClient, USE_YOUR_PROVIDER),
}
```

## Configuration

All API endpoints, parameters, and defaults are **hardcoded in clients**.
The config file only contains:
- **Credentials** (NOAA tokens)
- **Locations** (lat/lon + station IDs)

This eliminates configuration errors and makes the system more maintainable.

## Troubleshooting

**Import errors:**
```bash
# Make sure you're in the project root
cd "D:\DL Project"

# Activate venv
.venv\Scripts\activate

# Run with full path
python scripts/export.py --once
```

**No data returned:**
- Check your date range (some providers don't have future data)
- Verify NOAA token in config
- Check station IDs match your locations

**Missing dependencies:**
```bash
pip install -r requirements.txt
```

## License

MIT

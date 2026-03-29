# setup-sw-db

Solar and space weather database management system for downloading, validating, and registering solar observation data into PostgreSQL.

## Features

- **Solar Image Data**: Download and manage FITS images from LASCO (SOHO), SDO (AIA/HMI), and SECCHI (STEREO)
- **Space Weather Indices**: Ingest OMNI solar wind data (hourly, 1-min, 5-min resolution) and HPo geomagnetic indices (Hp30/Hp60)
- **FITS Validation**: Automated quality checks on solar image metadata and pixel data
- **Database Management**: PostgreSQL-based storage with composite primary keys, upsert support, and orphan cleanup

## Requirements

- Python 3
- PostgreSQL

### Python Dependencies

```
pandas
numpy
astropy
drms
sunpy
requests
urllib3
pyyaml
egghouse
```

## Setup

### 1. Environment Variables

Set database connection parameters:

```bash
export DB_HOST=localhost
export DB_USER=your_user
export DB_PASSWORD=your_password
```

### 2. Initialize Databases

```bash
python scripts/create_all_tables.py
```

This creates two PostgreSQL databases:
- `solar_images` - tables: `lasco`, `sdo`, `secchi`
- `space_weather` - tables: `omni_low_resolution`, `omni_high_resolution`, `omni_high_resolution_5min`, `hpo_hp30`, `hpo_hp60`

## Usage

### Download OMNI Solar Wind Data

```bash
# All resolutions
python scripts/download_omni.py --all --start 2020 --end 2024

# Specific resolution
python scripts/download_omni.py --lowres --start 2020 --end 2024
python scripts/download_omni.py --highres --highres-5min --start 2024 --end 2024
```

> At least one resolution flag (`--lowres`, `--highres`, `--highres-5min`, or `--all`) is required.

### Download HPo Geomagnetic Indices

```bash
# Full historical series (1985-present)
python scripts/download_hpo.py --all --mode complete

# Last 30 days (incremental update)
python scripts/download_hpo.py --all --mode nowcast
```

### Download Solar Images

```bash
# SDO (AIA + HMI)
python scripts/download_sdo.py --start-date 2024-01-01 --end-date 2024-01-31 --init-db
python scripts/download_sdo.py --days 7 --overwrite --parallel 4

# LASCO
python scripts/download_lasco.py --start-date 2024-01-01 --end-date 2024-01-31

# SECCHI
python scripts/download_secchi.py --start-date 2024-01-01 --end-date 2024-01-31
```

### Register Existing FITS Files

```bash
python scripts/register_sdo.py /path/to/fits/files --parallel 4
python scripts/register_lasco.py /path/to/fits/files
python scripts/register_secchi.py /path/to/fits/files
```

### Query Data

```bash
python scripts/query_sdo.py --help
```

### Download Files from URL List

```bash
python scripts/download_from_urls.py --help
```

## Project Structure

```
setup-sw-db/
├── configs/
│   ├── solar_images_config.yaml    # LASCO, SDO, SECCHI schema and download settings
│   └── space_weather_config.yaml   # OMNI data schema and download settings
├── core/
│   ├── cli.py                      # Shared CLI argument utilities
│   ├── database.py                 # DB creation, table management, insert/upsert
│   ├── download.py                 # HTTP download with retry and parallel support
│   ├── lasco.py                    # LASCO-specific query, download, metadata
│   ├── parse.py                    # OMNI/HPo data parsing, FITS datetime parsing
│   ├── query.py                    # DB query functions (best match, time range)
│   ├── sdo.py                      # SDO/JSOC query, FITS validation, TAI-UTC conversion
│   ├── secchi.py                   # SECCHI metadata extraction
│   └── utils.py                    # YAML config loader with env var substitution
├── scripts/
│   ├── create_all_tables.py        # Initialize all databases and tables
│   ├── download_omni.py            # Download OMNI solar wind data
│   ├── download_hpo.py             # Download HPo geomagnetic indices (Hp30/Hp60)
│   ├── download_sdo.py             # Download SDO images via JSOC
│   ├── download_lasco.py           # Download LASCO images via VSO
│   ├── download_secchi.py          # Download SECCHI images
│   ├── download_from_urls.py       # Generic URL-based file downloader
│   ├── register_sdo.py             # Validate and register SDO FITS files
│   ├── register_lasco.py           # Validate and register LASCO FITS files
│   ├── register_secchi.py          # Validate and register SECCHI FITS files
│   └── query_sdo.py                # Query SDO images from database
└── LICENSE
```

## Data Sources

| Source | Provider | Access Method |
|--------|----------|---------------|
| SDO/AIA, SDO/HMI | JSOC (Stanford) | `drms` client |
| LASCO | VSO / NRL Archive | `sunpy` Fido / HTTP |
| SECCHI | NASA SECCHI Archive | HTTP directory listing |
| OMNI | NASA SPDF | HTTP download |
| HPo (Hp30/Hp60) | GFZ Potsdam | HTTP download |

## License

MIT License (2025) - Eunsu Park

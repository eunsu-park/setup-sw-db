# Usage Guide

## Prerequisites

- Python 3
- PostgreSQL running and accessible
- Required Python packages:
  ```
  pandas numpy astropy drms sunpy requests urllib3 pyyaml egghouse
  ```

## Environment Variables

Set database connection parameters before running any script:

```bash
export DB_HOST=localhost    # PostgreSQL host (default: localhost)
export DB_USER=your_user    # PostgreSQL user (required)
export DB_PASSWORD=your_pw  # PostgreSQL password (required)
```

Port is fixed at `5432`.

---

## Quick Start

```bash
# 1. Initialize databases and tables
python scripts/create_all_tables.py

# 2. Download data (pick what you need)
python scripts/download_sdo.py --days 7
python scripts/download_lasco.py --days 7
python scripts/download_secchi.py --days 7
python scripts/download_omni.py --all --start 2020 --end 2024
python scripts/download_hpo.py --all --start 2020 --end 2024
```

---

## Scripts Reference

### Database Setup

#### `create_all_tables.py`

Initialize both databases (`solar_images`, `space_weather`) and all tables.

```bash
python scripts/create_all_tables.py           # Create if not exists
python scripts/create_all_tables.py --drop    # Drop and recreate all tables
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--drop` | False | Drop existing tables before creating |

---

### SDO (Solar Dynamics Observatory)

#### `download_sdo.py`

Download SDO/AIA and SDO/HMI FITS images from JSOC.

**Date Range Mode** (default):
```bash
python scripts/download_sdo.py --days 7
python scripts/download_sdo.py --start-date 2024-01-01 --end-date 2024-01-31
python scripts/download_sdo.py --telescope hmi --channels m_45s m_720s --days 30
```

**Target Time Mode** (find images near a specific event):
```bash
python scripts/download_sdo.py --target-time "2024-01-15 10:30:00" --time-range 30
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--start-date` | - | Start date (YYYY-MM-DD) |
| `--end-date` | - | End date (YYYY-MM-DD) |
| `--days` | 7 | Number of recent days to download |
| `--telescope` | aia | `aia` or `hmi` |
| `--channels` | all | Specific channels to download |
| `--parallel` | 4 | Number of parallel downloads |
| `--cadence` | 1h | Data cadence for JSOC query |
| `--overwrite` | False | Overwrite existing files |
| `--target-time` | - | Single target time (YYYY-MM-DD HH:MM:SS) |
| `--time-range` | 6 | Search ± N minutes around target time |
| `--skip-query` | False | Skip JSOC query, process existing files only |
| `--skip-db-check` | False | Skip DB existence check |
| `--init-db` | False | Initialize database before download |
| `--email` | from config | JSOC registered email |
| `--config` | configs/solar_images_config.yaml | Config file path |

**Available Channels**:
- AIA: `193`, `211`, `171`, `304`, `94`, `131`, `335`
- HMI: `m_45s`, `m_720s`

#### `query_sdo.py`

Query JSOC and save download URLs to a JSON file (for offline/deferred download).

```bash
python scripts/query_sdo.py --target-time "2024-01-15 10:30:00" --output urls.json
python scripts/query_sdo.py --target-time 2024-01-15 --telescope hmi --output hmi_urls.json
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--target-time` | **required** | Target time to query |
| `--output` | **required** | Output JSON file path |
| `--telescope` | aia | `aia` or `hmi` |
| `--channels` | all | Specific channels |
| `--time-range` | 6 | Search ± N minutes |
| `--include-spike` | False | Include AIA spike files |
| `--skip-db-check` | False | Always query JSOC |
| `--email` | from config | JSOC registered email |

> Note: JSOC URLs expire after ~24 hours.

#### `download_from_urls.py`

Download SDO files from a JSON URL list produced by `query_sdo.py`.

```bash
python scripts/download_from_urls.py --input urls.json
python scripts/download_from_urls.py --input urls.json --channels 193 211 --parallel 8
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--input` | **required** | JSON file from `query_sdo.py` |
| `--parallel` | 4 | Number of parallel downloads |
| `--overwrite` | False | Overwrite existing files |
| `--channels` | all in JSON | Only download specific channels |
| `--skip-process` | False | Download only, skip validation |
| `--skip-db` | False | Skip database registration |
| `--init-db` | False | Initialize database tables |

#### `register_sdo.py`

Scan, validate, and register existing SDO FITS files into the database.

```bash
python scripts/register_sdo.py                                    # Default download directory
python scripts/register_sdo.py /path/to/fits/files --parallel 8   # Custom directory
python scripts/register_sdo.py --clean-orphans                    # Remove stale DB records
python scripts/register_sdo.py --no-move --check-first 100        # Test run without moving files
```

| Argument | Default | Description |
|----------|---------|-------------|
| `scan_dir` (positional) | downloaded/ | Directory to scan for FITS files |
| `--parallel` | 4 | Parallel validation workers |
| `--batch-size` | 1000 | Records per DB batch insert |
| `--no-move` | False | Register only, don't move files |
| `--verbose` | False | Print details for each file |
| `--check-first` | 0 | Process only first N files (0 = all) |
| `--clean-orphans` | False | Delete DB records where file is missing |
| `--init-db` | False | Initialize database tables |

---

### LASCO (SOHO Coronagraph)

#### `download_lasco.py`

Download LASCO coronagraph FITS data.

```bash
python scripts/download_lasco.py --days 7                                        # NRL archive (default)
python scripts/download_lasco.py --cameras c2 c3 --start-date 2024-01-01 --end-date 2024-01-31
python scripts/download_lasco.py --realtime --days 3                             # UMBRA realtime server
python scripts/download_lasco.py --vso --cameras c2 --days 30                    # VSO via SunPy
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--start-date` | - | Start date (YYYY-MM-DD) |
| `--end-date` | - | End date (YYYY-MM-DD) |
| `--days` | 7 | Number of recent days |
| `--cameras` | [c2] | Cameras: `c1`, `c2`, `c3`, `c4` |
| `--overwrite` | False | Overwrite existing files |
| `--realtime` | False | Use UMBRA realtime server |
| `--vso` | False | Use VSO via SunPy Fido |
| `--init-db` | False | Initialize database tables |

**Data Sources**:
| Source | URL | Notes |
|--------|-----|-------|
| NRL Archive (default) | `https://lasco-www.nrl.navy.mil/lz/level_05/` | Complete, slower |
| UMBRA Realtime | `https://umbra.nascom.nasa.gov/pub/lasco/lastimage/level_05` | Recent data, faster |
| VSO | via SunPy Fido | Most flexible filtering |

> Mission start: 1995-12-08. Dates earlier than this are auto-adjusted.

#### `register_lasco.py`

Register existing LASCO FITS files into the database.

```bash
python scripts/register_lasco.py --cameras c2 c3
python scripts/register_lasco.py --cameras c1 c2 c3 c4 --verbose
python scripts/register_lasco.py --clean-orphans
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--cameras` | [c2] | Cameras to scan |
| `--batch-size` | 1000 | Records per batch insert |
| `--verbose` | False | Print parse failures |
| `--check-first` | 0 | Process only first N files |
| `--clean-orphans` | False | Delete stale DB records |
| `--init-db` | False | Initialize database tables |

---

### SECCHI (STEREO)

#### `download_secchi.py`

Download STEREO/SECCHI FITS data from NASA servers.

```bash
python scripts/download_secchi.py --days 7
python scripts/download_secchi.py --spacecrafts ahead behind --instruments cor1 cor2 euvi
python scripts/download_secchi.py --datatypes beacon --instruments hi_1 hi_2 --days 30
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--start-date` | - | Start date (YYYY-MM-DD) |
| `--end-date` | - | End date (YYYY-MM-DD) |
| `--days` | 7 | Number of recent days |
| `--datatypes` | [science] | `science` or `beacon` |
| `--spacecrafts` | [ahead] | `ahead` or `behind` |
| `--categories` | [img] | `img`, `seq`, `cal` |
| `--instruments` | [cor2] | `cor1`, `cor2`, `euvi`, `hi_1`, `hi_2` |
| `--overwrite` | False | Overwrite existing files |
| `--init-db` | False | Initialize database tables |

> Mission start: 2006-10-27. Dates earlier than this are auto-adjusted.

#### `register_secchi.py`

Register existing SECCHI FITS files into the database.

```bash
python scripts/register_secchi.py --instruments cor1 cor2 euvi
python scripts/register_secchi.py --spacecrafts ahead behind --instruments cor1 cor2 euvi hi_1 hi_2
python scripts/register_secchi.py --clean-orphans
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--datatypes` | [science] | Data types to scan |
| `--spacecrafts` | [ahead] | Spacecrafts to scan |
| `--instruments` | [cor2] | Instruments to scan |
| `--batch-size` | 1000 | Records per batch insert |
| `--verbose` | False | Print parse details |
| `--check-first` | 0 | Process only first N files |
| `--clean-orphans` | False | Delete stale DB records |
| `--init-db` | False | Initialize database tables |

---

### OMNI (Space Weather Indices)

#### `download_omni.py`

Download OMNI solar wind and geomagnetic index data from NASA SPDF.

```bash
python scripts/download_omni.py --all --start 2020 --end 2024
python scripts/download_omni.py --lowres --start 2020 --end 2024
python scripts/download_omni.py --highres --highres-5min --start 2024 --end 2024
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--lowres` | False | Download hourly data |
| `--highres` | False | Download 1-minute data |
| `--highres-5min` | False | Download 5-minute data |
| `--all` | False | Download all resolutions |
| `--start` | 2020 | Start year |
| `--end` | 2024 | End year |
| `--config` | configs/space_weather_config.yaml | Config file path |

> At least one resolution flag is required. Data is replaced per year.

---

### HPo (Geomagnetic Hp30/Hp60 Index)

#### `download_hpo.py`

Download Hp30/Hp60 geomagnetic activity indices from GFZ Potsdam.

```bash
python scripts/download_hpo.py --all --start 2020 --end 2024
python scripts/download_hpo.py --hp30 --start 1985 --end 2024
python scripts/download_hpo.py --all --nowcast
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--hp30` | False | Download Hp30 (30-min resolution) data |
| `--hp60` | False | Download Hp60 (60-min resolution) data |
| `--all` | False | Download both Hp30 and Hp60 |
| `--start` | 1985 | Start year |
| `--end` | current year | End year |
| `--nowcast` | False | Download last 30 days (incremental upsert) |
| `--config` | configs/space_weather_config.yaml | Config file path |

> At least one of `--hp30`, `--hp60`, or `--all` is required.
> Year-based mode replaces data per year. `--nowcast` mode upserts (skips duplicates).

**Data Source**: [GFZ Potsdam Hpo Index](https://kp.gfz.de/en/hp30-hp60/data)

---

### SW 30-min Aggregation & Event Extraction

#### `build_sw_30min.py`

Build the `sw_30min` aggregation table from OMNI 1-min and HPo 30-min data, or extract single event data.

```bash
python scripts/build_sw_30min.py build --start-year 2010 --end-year 2025
python scripts/build_sw_30min.py extract -t "2021-01-10 00:00:00" -b 5 -a 3 -o output.csv
```

| Subcommand | Argument | Default | Description |
|------------|----------|---------|-------------|
| `build` | `--start-year` | 2010 | Start year |
| | `--end-year` | 2025 | End year |
| `extract` | `-t` / `--time` | required | Reference time T (YYYY-MM-DD HH:MM:SS) |
| | `-b` / `--before` | 5 | Days before T |
| | `-a` / `--after` | 3 | Days after T |
| | `-o` / `--output` | - | Output CSV path (default: stdout) |

> See [sw_30min_spec.md](sw_30min_spec.md) for data specification.

#### `extract_sw_events.py`

Batch extract event windows over a time range. Each T produces an individual CSV file. Events with any NaN values are skipped.

```bash
python scripts/extract_sw_events.py \
  -s "2011-01-01 00:00:00" -e "2011-12-31 23:30:00" \
  -c 30 -b 5 -a 3 -o /path/to/output/
```

| Argument | Default | Description |
|----------|---------|-------------|
| `-s` / `--start` | required | Start time (YYYY-MM-DD HH:MM:SS) |
| `-e` / `--end` | required | End time (YYYY-MM-DD HH:MM:SS) |
| `-c` / `--cadence` | 30 | T iteration interval in minutes |
| `-b` / `--before` | 5 | Days before T |
| `-a` / `--after` | 3 | Days after T |
| `-o` / `--output-dir` | required | Output directory |

Output filenames: `YYYYMMDDHHMMSS.csv` (e.g., `20110101000000.csv`).

---

## Typical Workflows

### Initial Setup & Full Download

```bash
python scripts/create_all_tables.py
python scripts/download_sdo.py --start-date 2024-01-01 --end-date 2024-12-31 --parallel 8
python scripts/download_lasco.py --cameras c2 c3 --start-date 2024-01-01 --end-date 2024-12-31
python scripts/download_secchi.py --instruments cor1 cor2 euvi --start-date 2024-01-01 --end-date 2024-12-31
python scripts/download_omni.py --all --start 2020 --end 2024
python scripts/download_hpo.py --all --start 2020 --end 2024
```

### Query & Offline Download (SDO)

```bash
# Step 1: Query JSOC for URLs
python scripts/query_sdo.py --target-time "2024-01-15 10:30:00" --time-range 30 --output urls.json

# Step 2: Download later (within 24 hours)
python scripts/download_from_urls.py --input urls.json --parallel 8
```

### Bulk Registration (Pre-Downloaded Files)

```bash
python scripts/register_sdo.py /data/sdo_files --parallel 8
python scripts/register_lasco.py --cameras c1 c2 c3 c4
python scripts/register_secchi.py --spacecrafts ahead behind --instruments cor1 cor2 euvi hi_1 hi_2
```

### Daily Update (Cron Job)

```bash
python scripts/download_sdo.py --days 7 --parallel 8
python scripts/download_lasco.py --days 7
python scripts/download_secchi.py --days 7
python scripts/download_hpo.py --all --nowcast
```

### Cleanup Orphan Records

```bash
python scripts/register_sdo.py --clean-orphans
python scripts/register_lasco.py --clean-orphans
python scripts/register_secchi.py --clean-orphans
```

---

## Database Schema

### solar_images database

| Table | Primary Key | Unique | Indexes |
|-------|-------------|--------|---------|
| sdo | (telescope, channel, datetime) | file_path | datetime, telescope, quality |
| lasco | (camera, datetime) | file_path | datetime |
| secchi | (datatype, spacecraft, instrument, channel, datetime) | file_path | datetime, spacecraft, instrument |

### space_weather database

| Table | Primary Key | Resolution | Columns |
|-------|-------------|------------|---------|
| omni_low_resolution | datetime | Hourly | 55 |
| omni_high_resolution | datetime | 1-minute | 46 |
| omni_high_resolution_5min | datetime | 5-minute | 49 |
| hpo_hp30 | datetime | 30-minute | 7 |
| hpo_hp60 | datetime | 60-minute | 7 |
| sw_30min | datetime | 30-minute | 24 |

---

## File Organization

All downloaded files are organized into a date-based directory structure:

```
/opt/nas/archive/
├── sdo/
│   ├── aia/{YYYY}/{YYYYMMDD}/*.fits
│   ├── hmi/{YYYY}/{YYYYMMDD}/*.fits
│   ├── downloaded/          # Temporary download directory
│   ├── invalid_file/        # Unreadable FITS files
│   ├── invalid_header/      # Missing required headers
│   └── invalid_data/        # Data quality issues
├── solar_images/
│   ├── lasco/{camera}/{YYYY}/{YYYYMMDD}/*.fts
│   └── secchi/{datatype}/{spacecraft}/{instrument}/{YYYY}/{YYYYMMDD}/*.fts
└── space_weather/
    ├── omni/
    │   ├── low_resolution/
    │   ├── high_resolution/
    │   └── high_resolution_5min/
    └── hpo/
        ├── hp30/
        └── hp60/
```

---

## Configuration

Both config files are in `configs/` and support environment variable substitution with `${VAR}` or `${VAR:default}` syntax.

- `configs/solar_images_config.yaml` — SDO, LASCO, SECCHI schema and download settings
- `configs/space_weather_config.yaml` — OMNI schema and download settings

Override the config file path with `--config` on any script.

---

## Important Notes

1. **AIA Spike Files**: Automatically filtered out during download and registration (filenames containing 'spike').
2. **SDO Quality**: All quality levels are stored in the database. Use `require_quality_zero=True` when querying for science-grade data.
3. **TAI vs UTC**: SDO/HMI uses TAI timestamps in FITS headers. The system automatically converts to UTC (37-second offset) for database storage.
4. **JSOC URL Expiry**: URLs from `query_sdo.py` expire after ~24 hours. A warning is displayed if the JSON file is older.
5. **OMNI Replacement**: OMNI data is replaced per year — re-downloading a year overwrites previous records.
6. **Upsert Behavior**: All insert operations use ON CONFLICT DO NOTHING. Duplicate records are silently skipped.
7. **HPo Year-Based Mode**: Default mode downloads via GFZ JSON API per year, replacing data per year. Use `--nowcast` for incremental updates (last 30 days).

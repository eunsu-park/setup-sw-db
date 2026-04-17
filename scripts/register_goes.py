"""Scan archived GOES netCDF files and register them into PostgreSQL.

Walks the configured archive (per satellite + instrument), parses each
netCDF file into a DataFrame, and upserts into the goes_xrs / goes_mag /
goes_proton tables keyed on (satellite, datetime).
"""
import argparse
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    INSTRUMENT_TABLES,
    get_goes_year_dir,
    initialize_database,
    load_config,
    parse_goes_netcdf,
    upsert,
)

INSTRUMENT_KEYS = {
    "xrs": "goes_xrs",
    "mag": "goes_mag",
    "proton": "goes_proton",
}

_DATE_IN_FILENAME = re.compile(r"_d(\d{8})_")


def scan_netcdf_files(save_dir: str, satellites: list[int]) -> list[tuple[int, Path]]:
    """Collect all netCDF files for the given satellites under save_dir.

    Args:
        save_dir: Instrument archive root.
        satellites: Satellite numbers to include.

    Returns:
        Sorted list of (satellite, file_path) tuples.
    """
    root = Path(save_dir)
    if not root.exists():
        return []

    results: list[tuple[int, Path]] = []
    for satellite in satellites:
        sat_dir = root / f"g{satellite:02d}"
        if not sat_dir.exists():
            continue
        for path in sat_dir.rglob("*.nc"):
            results.append((satellite, path))

    results.sort(key=lambda t: (t[0], t[1].name))
    return results


def process_instrument(instrument: str, satellites: list[int],
                       download_config: dict, db_config: dict,
                       verbose: bool) -> dict:
    """Register all archived files for an instrument.

    Args:
        instrument: 'xrs' | 'mag' | 'proton'.
        satellites: Satellite numbers to include.
        download_config: Full download_config from YAML.
        db_config: DB connection config.
        verbose: Per-file logging.

    Returns:
        Dict with counts: files, parsed, failed, rows_inserted.
    """
    config_key = INSTRUMENT_KEYS[instrument]
    instrument_config = download_config.get(config_key)
    if instrument_config is None:
        print(f"  No download_config for {config_key}")
        return {"files": 0, "parsed": 0, "failed": 0, "rows_inserted": 0}

    save_dir = instrument_config["save_dir"]
    table = INSTRUMENT_TABLES[instrument]

    files = scan_netcdf_files(save_dir, satellites)
    print(f"  Found {len(files)} files in {save_dir}")

    counts = {"files": len(files), "parsed": 0, "failed": 0, "rows_inserted": 0}

    for satellite, path in files:
        try:
            df = parse_goes_netcdf(instrument, str(path), satellite)
        except Exception as e:
            counts["failed"] += 1
            if verbose:
                print(f"    parse error: {path.name}: {e}")
            continue

        if df.empty:
            counts["failed"] += 1
            if verbose:
                print(f"    empty: {path.name}")
            continue

        try:
            inserted = upsert(df, table, db_config,
                              conflict_columns=["satellite", "datetime"])
        except Exception as e:
            counts["failed"] += 1
            if verbose:
                print(f"    upsert error: {path.name}: {e}")
            continue

        counts["parsed"] += 1
        counts["rows_inserted"] += inserted
        if verbose:
            print(f"    {path.name}: {inserted} rows")

    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Register archived GOES netCDF files into the database",
    )

    parser.add_argument("--instrument", type=str, required=True,
                        choices=["xrs", "mag", "proton", "all"],
                        help="Instrument to register")
    parser.add_argument("--satellites", type=int, nargs="+", required=True,
                        help="Satellite numbers, e.g. 16 17 18")
    parser.add_argument("--init-db", action="store_true",
                        help="Initialize DB tables before registering")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print details for each file")
    parser.add_argument("--config", type=str,
                        default="configs/space_weather_config.yaml",
                        help="Config file path")

    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config["db_config"]
    download_config = config["download_config"]

    if args.init_db:
        initialize_database(db_config, config["schema_config"])

    if args.instrument == "all":
        instruments = ["xrs", "mag", "proton"]
    else:
        instruments = [args.instrument]

    print("GOES NetCDF Registration")
    print(f"Satellites: {args.satellites}")
    print(f"Instruments: {instruments}")
    print()

    totals = {"files": 0, "parsed": 0, "failed": 0, "rows_inserted": 0}
    for instrument in instruments:
        print(f"[{instrument}]")
        stats = process_instrument(instrument, args.satellites,
                                   download_config, db_config, args.verbose)
        for key, value in stats.items():
            totals[key] += value
        print(f"  files={stats['files']} parsed={stats['parsed']} "
              f"failed={stats['failed']} rows={stats['rows_inserted']}")
        print()

    print("=" * 50)
    for key, value in totals.items():
        print(f"Total {key}: {value}")


if __name__ == "__main__":
    main()

"""GOES netCDF downloader from NOAA NCEI.

Fetches daily L2 netCDF files for XRS, MAG, and proton instruments across
selected satellites and date ranges. Files are saved to the configured
archive; DB registration is a separate step (register_goes.py).
"""
import argparse
from datetime import date, datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    download_files_parallel,
    get_goes_save_path,
    initialize_database,
    list_goes_files,
    load_config,
)

INSTRUMENT_KEYS = {
    "xrs": "goes_xrs",
    "mag": "goes_mag",
    "proton": "goes_proton",
}


def _parse_date(value: str) -> date:
    """Parse YYYY-MM-DD into a date object."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def process_satellite_instrument(satellite: int, instrument: str,
                                  start_date: date, end_date: date,
                                  download_config: dict,
                                  parallel: int, overwrite: bool) -> dict:
    """Download one instrument's files for one satellite over a date range.

    Args:
        satellite: Satellite number.
        instrument: 'xrs' | 'mag' | 'proton'.
        start_date: First date (inclusive).
        end_date: Last date (inclusive).
        download_config: Full download_config section from YAML.
        parallel: Concurrent downloads.
        overwrite: Overwrite already-present files.

    Returns:
        Dict with keys: listed, downloaded, failed, skipped.
    """
    config_key = INSTRUMENT_KEYS[instrument]
    instrument_config = download_config.get(config_key)
    if instrument_config is None:
        print(f"  No download_config for {config_key}")
        return {"listed": 0, "downloaded": 0, "failed": 0, "skipped": 0}

    save_dir = instrument_config["save_dir"]

    print(f"  Listing NCEI: satellite=g{satellite:02d}, "
          f"instrument={instrument}, {start_date} .. {end_date}")
    listing = list_goes_files(satellite, instrument_config, start_date, end_date)
    print(f"  Found {len(listing)} files")

    tasks: list[tuple[str, str]] = []
    skipped = 0
    for url, filename in listing:
        save_path = get_goes_save_path(save_dir, satellite, filename)
        if save_path.exists() and not overwrite:
            skipped += 1
            continue
        tasks.append((url, str(save_path)))

    if not tasks:
        print(f"  Nothing to download (skipped {skipped})")
        return {"listed": len(listing), "downloaded": 0, "failed": 0,
                "skipped": skipped}

    print(f"  Downloading {len(tasks)} files (skipped {skipped})")
    success, failed = download_files_parallel(
        tasks, max_workers=parallel, overwrite=overwrite,
    )
    print(f"  Downloaded: {success}, failed: {failed}")
    return {
        "listed": len(listing),
        "downloaded": success,
        "failed": failed,
        "skipped": skipped,
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="GOES netCDF downloader")

    parser.add_argument("--instrument", type=str, required=True,
                        choices=["xrs", "mag", "proton", "all"],
                        help="Instrument to download")
    parser.add_argument("--satellites", type=int, nargs="+", required=True,
                        help="Satellite numbers, e.g. 16 17 18")
    parser.add_argument("--start-date", type=str, required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True,
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--parallel", type=int, default=4,
                        help="Concurrent downloads (default: 4)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing files")
    parser.add_argument("--init-db", action="store_true",
                        help="Initialize DB tables before downloading")
    parser.add_argument("--config", type=str,
                        default="configs/space_weather_config.yaml",
                        help="Config file path")

    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config["db_config"]
    download_config = config["download_config"]

    if args.init_db:
        initialize_database(db_config, config["schema_config"])

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)

    if args.instrument == "all":
        instruments = ["xrs", "mag", "proton"]
    else:
        instruments = [args.instrument]

    print("GOES NetCDF Downloader")
    print(f"Satellites: {args.satellites}")
    print(f"Instruments: {instruments}")
    print(f"Date range: {start_date} .. {end_date}")
    print(f"Parallel: {args.parallel}")
    print()

    totals = {"listed": 0, "downloaded": 0, "failed": 0, "skipped": 0}
    for satellite in args.satellites:
        for instrument in instruments:
            print(f"[g{satellite:02d} / {instrument}]")
            stats = process_satellite_instrument(
                satellite, instrument, start_date, end_date,
                download_config, args.parallel, args.overwrite,
            )
            for key, value in stats.items():
                totals[key] += value
            print()

    print("=" * 50)
    for key, value in totals.items():
        print(f"Total {key}: {value}")


if __name__ == "__main__":
    main()

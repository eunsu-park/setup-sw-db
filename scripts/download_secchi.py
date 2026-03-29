"""STEREO SECCHI FITS file downloader with DB integration.

Downloads SECCHI data from NASA servers and registers to unified secchi table.
"""
import argparse
import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    load_config,
    download_file,
    list_remote_files,
    upsert,
    add_date_arguments,
    add_db_arguments,
    parse_date_range,
    initialize_database,
    get_secchi_save_dir,
    get_secchi_record,
)

# SECCHI data source configuration
URL_ROOT_SCIENCE = "https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/L0"
URL_ROOT_BEACON = "https://stereo-ssc.nascom.nasa.gov/data/beacon"
MISSION_START = datetime.date(2006, 10, 27)

# Valid options
DATATYPES = ['science', 'beacon']
SPACECRAFTS = ['ahead', 'behind']
CATEGORIES = ['img', 'seq', 'cal']
INSTRUMENTS = ['hi_1', 'hi_2', 'cor1', 'cor2', 'euvi']


def get_remote_url(date: datetime.date, datatype: str, spacecraft: str,
                   category: str, instrument: str) -> str:
    """Build remote directory URL.

    Args:
        date: Date to download.
        datatype: Data type (science or beacon).
        spacecraft: Spacecraft name (ahead or behind).
        category: Category (img, seq, cal).
        instrument: Instrument name.

    Returns:
        URL string for the remote directory.
    """
    date_str = f"{date:%Y%m%d}"

    if datatype == "science":
        sc_letter = spacecraft[0]  # 'a' or 'b'
        return f"{URL_ROOT_SCIENCE}/{sc_letter}/{category}/{instrument}/{date_str}"
    else:
        return f"{URL_ROOT_BEACON}/{spacecraft}/secchi/{category}/{instrument}/{date_str}"


def process_date(date: datetime.date, datatype: str, spacecraft: str,
                 category: str, instrument: str, download_root: str,
                 db_config: dict, overwrite: bool = False) -> tuple[int, int, int]:
    """Process a single date: download files and insert to DB.

    Args:
        date: Date to process.
        datatype: Data type.
        spacecraft: Spacecraft name.
        category: Category.
        instrument: Instrument name.
        download_root: Root directory for downloads.
        db_config: Database configuration.
        overwrite: Whether to overwrite existing files.

    Returns:
        Tuple of (downloaded_count, skipped_count, inserted_count).
    """
    url = get_remote_url(date, datatype, spacecraft, category, instrument)
    save_dir = get_secchi_save_dir(download_root, datatype, spacecraft, instrument, date)

    # Get file list from remote
    files = list_remote_files(url, extension=".fts")
    if not files:
        return 0, 0, 0

    # Get existing files in local date directory
    if save_dir.exists():
        existing_files = set(f.name for f in save_dir.glob("*.fts"))
    else:
        existing_files = set()

    downloaded = 0
    skipped = 0
    records = []

    for filename in files:
        save_path = save_dir / filename

        # Check if file exists in local date directory
        if filename in existing_files and not overwrite:
            skipped += 1
            # Add existing file to records for DB sync
            record = get_secchi_record(str(save_path), datatype, spacecraft, instrument)
            if record:
                records.append(record)
            continue

        file_url = f"{url}/{filename}"

        # Download file
        if download_file(file_url, str(save_path), overwrite=overwrite):
            downloaded += 1

            # Create record
            record = get_secchi_record(str(save_path), datatype, spacecraft, instrument)
            if record:
                records.append(record)

    # Insert to database
    if records:
        import pandas as pd
        df = pd.DataFrame(records)
        inserted = upsert(df, 'secchi', db_config,
                          conflict_columns=['datatype', 'spacecraft', 'instrument',
                                            'channel', 'datetime'])
    else:
        inserted = 0

    return downloaded, skipped, inserted


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='STEREO SECCHI FITS Downloader')

    # Common arguments
    add_date_arguments(parser)
    add_db_arguments(parser)

    # Data selection options
    parser.add_argument('--datatypes', type=str, nargs='+', default=['science'],
                        choices=DATATYPES,
                        help='Data types (default: science)')
    parser.add_argument('--spacecrafts', type=str, nargs='+', default=['ahead'],
                        choices=SPACECRAFTS,
                        help='Spacecrafts (default: ahead)')
    parser.add_argument('--categories', type=str, nargs='+', default=['img'],
                        choices=CATEGORIES,
                        help='Categories (default: img)')
    parser.add_argument('--instruments', type=str, nargs='+', default=['cor2'],
                        choices=INSTRUMENTS,
                        help='Instruments (default: cor2)')

    # Download options
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']

    # Get download root
    download_config = config['download_config'].get('secchi', {})
    download_root = download_config.get('download_root', '/opt/archive/solar_images/secchi')

    # Initialize database if requested
    if args.init_db:
        initialize_database(db_config, config['schema_config'])

    # Parse date range
    start_date, end_date = parse_date_range(args, MISSION_START)

    print("STEREO SECCHI FITS Downloader")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Datatypes: {args.datatypes}")
    print(f"Spacecrafts: {args.spacecrafts}")
    print(f"Categories: {args.categories}")
    print(f"Instruments: {args.instruments}")
    print(f"Download root: {download_root}")
    print()

    # Process each combination
    total_downloaded = 0
    total_skipped = 0
    total_inserted = 0

    current_date = start_date
    while current_date <= end_date:
        for datatype in args.datatypes:
            for spacecraft in args.spacecrafts:
                for category in args.categories:
                    for instrument in args.instruments:
                        print(f"Processing {current_date} / {datatype} / {spacecraft} / {category} / {instrument}...", end=" ")

                        downloaded, skipped, inserted = process_date(
                            current_date, datatype, spacecraft, category, instrument,
                            download_root, db_config, args.overwrite
                        )

                        print(f"downloaded: {downloaded}, skipped: {skipped}, inserted: {inserted}")
                        total_downloaded += downloaded
                        total_skipped += skipped
                        total_inserted += inserted

        current_date += datetime.timedelta(days=1)

    print()
    print(f"Total: {total_downloaded} downloaded, {total_skipped} skipped, {total_inserted} inserted")


if __name__ == '__main__':
    main()

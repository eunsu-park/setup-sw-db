"""LASCO FITS file downloader with DB integration.

Downloads LASCO coronagraph data from NRL archive, UMBRA realtime server,
or VSO (Virtual Solar Observatory) and registers to unified lasco table.
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
    query_vso_lasco,
    download_vso_lasco,
    get_lasco_save_dir,
    lasco_file_exists,
    get_lasco_record,
)

# LASCO data source configuration
URL_ROOT_ARCHIVE = "https://lasco-www.nrl.navy.mil/lz/level_05"
URL_ROOT_REALTIME = "https://umbra.nascom.nasa.gov/pub/lasco/lastimage/level_05"
MISSION_START = datetime.date(1995, 12, 8)


def get_remote_url(year: int, month: int, day: int, camera: str,
                   realtime: bool = False) -> str:
    """Build remote directory URL for a given date and camera.

    Args:
        year: Year (e.g., 2024).
        month: Month (1-12).
        day: Day (1-31).
        camera: Camera name (c2, c3, etc.).
        realtime: If True, use UMBRA realtime server; otherwise use NRL archive.

    Returns:
        URL string for the remote directory.
    """
    url_root = URL_ROOT_REALTIME if realtime else URL_ROOT_ARCHIVE
    year_short = year % 100
    return f"{url_root}/{year_short:02d}{month:02d}{day:02d}/{camera}"


def get_save_dir(download_root: str, camera: str, year: int,
                 month: int, day: int) -> Path:
    """Build local save directory path.

    Args:
        download_root: Root directory for downloads.
        camera: Camera name.
        year: Year.
        month: Month.
        day: Day.

    Returns:
        Path object for the save directory.
    """
    return Path(download_root) / camera / f"{year:04d}" / f"{year:04d}{month:02d}{day:02d}"


def create_record(file_path: str, camera: str) -> dict | None:
    """Create a database record from FITS file.

    Args:
        file_path: Path to FITS file.
        camera: Camera name.

    Returns:
        Dict ready for DB insertion or None if parsing failed.
    """
    return get_lasco_record(file_path, camera=camera)


def process_date(date: datetime.date, camera: str, download_root: str,
                 db_config: dict, overwrite: bool = False,
                 realtime: bool = False) -> tuple[int, int]:
    """Process a single date: download files and insert to DB.

    Args:
        date: Date to process.
        camera: Camera name.
        download_root: Root directory for downloads.
        db_config: Database configuration.
        overwrite: Whether to overwrite existing files.
        realtime: If True, use UMBRA realtime server.

    Returns:
        Tuple of (downloaded_count, inserted_count).
    """
    url = get_remote_url(date.year, date.month, date.day, camera, realtime)
    save_dir = get_save_dir(download_root, camera, date.year, date.month, date.day)

    # Get file list from remote
    files = list_remote_files(url, extension=".fts")
    if not files:
        return 0, 0

    downloaded = 0
    records = []

    for filename in files:
        # Check if file already exists anywhere under camera directory
        existing_path = lasco_file_exists(filename, download_root, camera)
        if existing_path and not overwrite:
            # File exists, add to records for DB sync
            record = create_record(existing_path, camera)
            if record:
                records.append(record)
            continue

        file_url = f"{url}/{filename}"
        save_path = save_dir / filename

        # Download file
        if download_file(file_url, str(save_path), overwrite=overwrite):
            downloaded += 1

            # Create record
            record = create_record(str(save_path), camera)
            if record:
                records.append(record)

    # Insert to database
    if records:
        import pandas as pd
        df = pd.DataFrame(records)
        inserted = upsert(df, 'lasco', db_config,
                          conflict_columns=['camera', 'datetime'])
    else:
        inserted = 0

    return downloaded, inserted


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='LASCO FITS Downloader')

    # Common arguments
    add_date_arguments(parser)
    add_db_arguments(parser)

    # Camera options
    parser.add_argument('--cameras', type=str, nargs='+', default=['c2'],
                        choices=['c1', 'c2', 'c3', 'c4'],
                        help='LASCO cameras (default: c2)')

    # Download options
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files')
    parser.add_argument('--realtime', action='store_true',
                        help='Use UMBRA realtime server instead of NRL archive')
    parser.add_argument('--vso', action='store_true',
                        help='Use VSO (Virtual Solar Observatory) via SunPy Fido')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']

    # Get download root
    download_config = config['download_config'].get('lasco', {})
    download_root = download_config.get('download_root', '/opt/archive/solar_images/lasco')

    # Initialize database if requested
    if args.init_db:
        initialize_database(db_config, config['schema_config'])

    # Parse date range
    start_date, end_date = parse_date_range(args, MISSION_START)

    # Determine server type
    if args.vso:
        server_type = "VSO (SunPy Fido)"
    elif args.realtime:
        server_type = "UMBRA (realtime)"
    else:
        server_type = "NRL (archive)"

    print("LASCO FITS Downloader")
    print(f"Server: {server_type}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Cameras: {args.cameras}")
    print(f"Download root: {download_root}")
    print()

    # VSO mode
    if args.vso:
        total_downloaded = 0
        total_skipped = 0
        total_inserted = 0

        current_date = start_date
        while current_date <= end_date:
            for camera in args.cameras:
                print(f"Processing {current_date} / {camera} (VSO)...", end=" ")

                # 1. Query VSO for this date only
                results = query_vso_lasco(camera, current_date, current_date)
                if not results or len(results) == 0:
                    print()
                    continue

                # 2. Get existing files in local date directory
                save_dir = get_lasco_save_dir(download_root, camera, current_date)
                if save_dir.exists():
                    existing_files = set(f.name for f in save_dir.glob("*.fts"))
                else:
                    existing_files = set()

                # 3. Download to target directory (skip existing)
                downloaded, skipped, file_paths = download_vso_lasco(
                    results, str(save_dir), camera, args.overwrite, existing_files
                )
                total_downloaded += downloaded
                total_skipped += skipped

                # 4. Parse downloaded files and insert to DB
                records = []
                for file_path in file_paths:
                    record = create_record(str(file_path), camera)
                    if record:
                        records.append(record)

                # Also add existing files to DB (sync)
                for filename in existing_files:
                    file_path = save_dir / filename
                    record = create_record(str(file_path), camera)
                    if record:
                        records.append(record)

                if records:
                    import pandas as pd
                    df = pd.DataFrame(records)
                    inserted = upsert(df, 'lasco', db_config,
                                      conflict_columns=['camera', 'datetime'])
                else:
                    inserted = 0

                print(f"downloaded: {downloaded}, skipped: {skipped}, inserted: {inserted}")
                total_inserted += inserted

            current_date += datetime.timedelta(days=1)

        print()
        print(f"Total: {total_downloaded} downloaded, {total_skipped} skipped, {total_inserted} inserted")
        return

    # NRL/UMBRA mode
    total_downloaded = 0
    total_inserted = 0

    current_date = start_date
    while current_date <= end_date:
        for camera in args.cameras:
            print(f"Processing {current_date} / {camera}...", end=" ")

            downloaded, inserted = process_date(
                current_date, camera, download_root, db_config,
                args.overwrite, args.realtime
            )

            print(f"downloaded: {downloaded}, inserted: {inserted}")
            total_downloaded += downloaded
            total_inserted += inserted

        current_date += datetime.timedelta(days=1)

    print()
    print(f"Total: {total_downloaded} files downloaded, {total_inserted} records inserted")


if __name__ == '__main__':
    main()

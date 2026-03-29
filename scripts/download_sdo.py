"""SDO FITS file downloader with JSOC query and DB integration.

Uses unified sdo table with telescope + channel structure.
All valid files are stored in DB; best match selection is done at query time.
"""
import argparse
import datetime
import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    load_config,
    upsert,
    query_jsoc_v2,
    query_jsoc_time_range,
    validate_fits,
    get_target_path,
    file_exists_anywhere,
    check_db_exists_in_range,
    tai_to_utc,
    add_date_arguments,
    add_download_arguments,
    add_db_arguments,
    parse_date_range,
    initialize_database,
    download_files_parallel,
)

# Telescope + channel structure
TELESCOPES = {
    'aia': ['193', '211', '171', '304', '94', '131', '335'],
    'hmi': ['m_45s', 'm_720s'],
}


def is_spike_file(filename: str) -> bool:
    """Check if filename contains 'spike' (AIA spike files to exclude).

    Args:
        filename: Filename to check.

    Returns:
        True if file is a spike file, False otherwise.
    """
    return 'spike' in filename.lower()


def process_downloaded_files(download_dir: Path, telescope: str,
                             download_root: str, dir_config: dict,
                             db_config: dict) -> dict:
    """Process downloaded files: validate and move to appropriate directories.

    All valid files are stored in DB and moved to aia/hmi directories.
    Best match selection is done at query time, not during download.

    Args:
        download_dir: Directory containing downloaded files.
        telescope: Telescope name ('aia' or 'hmi').
        download_root: Root download directory.
        dir_config: Directory configuration.
        db_config: Database configuration.

    Returns:
        Dict with counts: valid, invalid_file, invalid_header, invalid_data.
    """
    import pandas as pd

    root = Path(download_root)
    counts = {
        'valid': 0,
        'invalid_file': 0,
        'invalid_header': 0,
        'invalid_data': 0,
    }

    fits_files = list(download_dir.glob("*.fits"))
    if not fits_files:
        return counts

    # Filter out spike files for AIA
    if telescope == 'aia':
        original_count = len(fits_files)
        fits_files = [f for f in fits_files if not is_spike_file(f.name)]
        spike_count = original_count - len(fits_files)
        if spike_count > 0:
            print(f"  Skipped {spike_count} spike files")

    print(f"  Processing {len(fits_files)} downloaded files...")

    records = []

    for file_path in fits_files:
        # Validate file (don't filter by quality - store all valid files)
        result = validate_fits(str(file_path), check_quality=False)

        if isinstance(result, str):
            # Error case: move to appropriate directory
            if result in dir_config:
                target_dir = root / dir_config[result]
                target_dir.mkdir(parents=True, exist_ok=True)
                target_path = target_dir / file_path.name
                shutil.move(str(file_path), str(target_path))
            counts[result] = counts.get(result, 0) + 1
            continue

        # Valid file with metadata
        dt = tai_to_utc(result['datetime'], telescope)
        target_dir = get_target_path(download_root, telescope, dt)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / file_path.name

        shutil.move(str(file_path), str(target_path))

        records.append({
            'telescope': result.get('telescope', telescope),
            'channel': result.get('channel'),
            'datetime': dt,
            'file_path': str(target_path),
            'quality': result.get('quality'),
            'wavelength': result.get('wavelength'),
            'exposure_time': None,
        })

        counts['valid'] += 1

    # Insert to database
    if records:
        df = pd.DataFrame(records)
        conflict_cols = ['telescope', 'channel', 'datetime']
        inserted = upsert(df, 'sdo', db_config, conflict_columns=conflict_cols)
        print(f"  Inserted {inserted} records to database")

    return counts


def process_target_time(target_time: datetime.datetime,
                        telescope: str, channel: str,
                        download_dir: Path, download_root: str,
                        dir_config: dict, db_config: dict,
                        jsoc_config: dict, time_range: int,
                        overwrite: bool, parallel: int,
                        skip_db_check: bool = False) -> dict:
    """Process target time mode: download all valid files for a specific time.

    All valid files are stored in DB. Best match selection is done
    at query time, not during download.

    Args:
        target_time: Target time (UTC).
        telescope: Telescope name (aia, hmi).
        channel: Channel name.
        download_dir: Download directory.
        download_root: Root download directory.
        dir_config: Directory configuration.
        db_config: Database configuration.
        jsoc_config: JSOC configuration.
        time_range: Search range in minutes (±).
        overwrite: Whether to overwrite existing files.
        parallel: Number of parallel downloads.
        skip_db_check: If True, skip DB existence check.

    Returns:
        Dict with status info.
    """
    result = {
        'downloaded': 0,
        'valid': 0,
        'invalid': 0,
        'skipped_db': False,
    }

    # Check DB for existing data in time range
    if not skip_db_check:
        if check_db_exists_in_range(
            target_time, time_range, 'sdo', db_config,
            telescope=telescope, channel=channel
        ):
            print(f"  Already in DB (±{time_range}min), skipping")
            result['skipped_db'] = True
            return result

    # Query JSOC for files within ± time_range minutes
    print(f"  Querying JSOC for {target_time} ± {time_range} min...")
    files = query_jsoc_time_range(f"{telescope}_{channel}", target_time, time_range, jsoc_config)

    if not files:
        print("  No files found in time range")
        return result

    # Download files (skip spike files for AIA)
    download_tasks = []
    spike_skipped = 0
    for file_info in files:
        url = file_info['url']
        filename = file_info['filename']

        # Skip spike files for AIA
        if telescope == 'aia' and is_spike_file(filename):
            spike_skipped += 1
            continue

        # Check if file exists anywhere
        existing = file_exists_anywhere(filename, download_root, dir_config)
        if existing and not overwrite:
            continue

        save_path = str(download_dir / filename)
        download_tasks.append((url, save_path))

    if spike_skipped > 0:
        print(f"  Skipped {spike_skipped} spike files")

    if download_tasks:
        print(f"  Downloading {len(download_tasks)} files...")
        success, failed = download_files_parallel(
            download_tasks, max_workers=parallel, overwrite=overwrite
        )
        result['downloaded'] = success
        print(f"  Downloaded: {success} files, failed: {failed}")

    # Process downloaded files (all valid files stored)
    counts = process_downloaded_files(
        download_dir, telescope, download_root, dir_config, db_config
    )

    result['valid'] = counts['valid']
    result['invalid'] = counts['invalid_file'] + counts['invalid_header'] + counts['invalid_data']

    print(f"  Valid: {counts['valid']}, Invalid: {result['invalid']}")

    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='SDO FITS Downloader')

    # Common arguments
    add_date_arguments(parser)
    add_download_arguments(parser)
    add_db_arguments(parser)

    # Telescope/channel options
    parser.add_argument('--telescope', type=str, choices=['aia', 'hmi'],
                        default='aia',
                        help='Telescope to download (default: aia)')
    parser.add_argument('--channels', type=str, nargs='+',
                        help='Channels to download (default: all for telescope)')

    # Query options
    parser.add_argument('--cadence', type=str, default=None,
                        help='Data cadence (default: from config, usually 1h)')
    parser.add_argument('--skip-query', action='store_true',
                        help='Skip JSOC query, only process existing downloaded files')

    # Target time mode
    parser.add_argument('--target-time', type=str, default=None,
                        help='Target time for single-time mode (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)')
    parser.add_argument('--time-range', type=int, default=6,
                        help='Search range in minutes for target-time mode (default: 6)')
    parser.add_argument('--skip-db-check', action='store_true',
                        help='Skip DB check in target-time mode (default: check DB first)')

    # JSOC options
    parser.add_argument('--email', type=str, default=None,
                        help='JSOC registered email (overrides config)')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']

    # Get download config
    download_config = config['download_config'].get('sdo', {})
    download_root = download_config.get('download_root', '/opt/archive/sdo')
    dir_config = download_config.get('dirs', {
        'aia': 'aia',
        'hmi': 'hmi',
        'downloaded': 'downloaded',
        'invalid_file': 'invalid_file',
        'invalid_header': 'invalid_header',
        'invalid_data': 'invalid_data',
    })

    jsoc_config = config['jsoc_config']

    # Override email from CLI if provided
    if args.email:
        jsoc_config['email'] = args.email

    cadence = args.cadence or jsoc_config.get('default_cadence', '1h')

    telescope = args.telescope
    channels = args.channels or TELESCOPES.get(telescope, [])

    # Initialize database if requested
    if args.init_db:
        initialize_database(db_config, config['schema_config'])

    # Create directories
    root = Path(download_root)
    download_dir = root / dir_config['downloaded']
    download_dir.mkdir(parents=True, exist_ok=True)

    # Target time mode
    if args.target_time:
        # Parse target time
        try:
            if ' ' in args.target_time:
                target_time = datetime.datetime.strptime(args.target_time, '%Y-%m-%d %H:%M:%S')
            else:
                target_time = datetime.datetime.strptime(args.target_time, '%Y-%m-%d')
        except ValueError:
            print(f"Error: Invalid target time format: {args.target_time}")
            print("Use YYYY-MM-DD HH:MM:SS or YYYY-MM-DD")
            return

        print("SDO FITS Downloader (Target Time Mode)")
        print(f"Target time: {target_time}")
        print(f"Time range: ± {args.time_range} minutes")
        print(f"Telescope: {telescope}")
        print(f"Channels: {channels}")
        print(f"Download root: {download_root}")
        print()

        for channel in channels:
            print(f"[{telescope.upper()} / {channel}]")
            process_target_time(
                target_time, telescope, channel, download_dir, download_root,
                dir_config, db_config, jsoc_config, args.time_range,
                args.overwrite, args.parallel, args.skip_db_check
            )
            print()

        return

    # Regular mode (date range)
    start_date, end_date = parse_date_range(args)

    print("SDO FITS Downloader")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Telescope: {telescope}")
    print(f"Channels: {channels}")
    print(f"Cadence: {cadence}")
    print(f"Download root: {download_root}")
    print(f"Parallel downloads: {args.parallel}")
    print()

    # Process each channel
    total_stats = {
        'downloaded': 0,
        'skipped': 0,
        'valid': 0,
        'invalid_file': 0,
        'invalid_header': 0,
        'invalid_data': 0,
    }

    for channel in channels:
        print(f"[{telescope.upper()} / {channel}]")

        if not args.skip_query:
            # Query JSOC
            print("  Querying JSOC...")
            files = query_jsoc_v2(telescope, channel, start_date, end_date,
                                  cadence, jsoc_config)

            if not files:
                print("  No files found")
                print()
                continue

            # Filter out existing files
            download_tasks = []
            skipped = 0
            spike_skipped = 0

            for file_info in files:
                url = file_info['url']
                filename = file_info['filename']

                # Skip spike files for AIA
                if telescope == 'aia' and is_spike_file(filename):
                    spike_skipped += 1
                    continue

                # Check if file exists anywhere
                existing = file_exists_anywhere(filename, download_root, dir_config)
                if existing and not args.overwrite:
                    skipped += 1
                    continue

                save_path = str(download_dir / filename)
                download_tasks.append((url, save_path))

            if spike_skipped > 0:
                print(f"  Skipped {spike_skipped} spike files")
            print(f"  Files to download: {len(download_tasks)}, skipped: {skipped}")
            total_stats['skipped'] += skipped

            # Download files in parallel
            if download_tasks:
                print("  Downloading...")
                success, failed = download_files_parallel(
                    download_tasks, max_workers=args.parallel, overwrite=args.overwrite
                )
                print(f"  Downloaded: {success} files, failed: {failed}")
                total_stats['downloaded'] += success

        # Process downloaded files
        counts = process_downloaded_files(
            download_dir, telescope, download_root, dir_config, db_config
        )

        print(f"  Valid: {counts['valid']}, "
              f"Invalid file: {counts['invalid_file']}, "
              f"Invalid header: {counts['invalid_header']}, "
              f"Invalid data: {counts['invalid_data']}")

        total_stats['valid'] += counts['valid']
        total_stats['invalid_file'] += counts['invalid_file']
        total_stats['invalid_header'] += counts['invalid_header']
        total_stats['invalid_data'] += counts['invalid_data']

        print()

    print("=" * 50)
    print(f"Total downloaded: {total_stats['downloaded']}")
    print(f"Total skipped: {total_stats['skipped']}")
    print(f"Total valid: {total_stats['valid']}")
    print(f"Total invalid file: {total_stats['invalid_file']}")
    print(f"Total invalid header: {total_stats['invalid_header']}")
    print(f"Total invalid data: {total_stats['invalid_data']}")


if __name__ == '__main__':
    main()

"""Download SDO files from URL list JSON and process them.

Reads a JSON file produced by query_sdo.py, downloads the files,
validates them, and registers to the unified sdo table.
"""
import argparse
import datetime
import json
import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    load_config,
    download_files_parallel,
    file_exists_anywhere,
    validate_fits,
    get_target_path,
    check_db_exists_in_range,
    upsert,
    tai_to_utc,
    initialize_database,
)


def is_spike_file(filename: str) -> bool:
    """Check if filename contains 'spike' (AIA spike files to exclude).

    Args:
        filename: Filename to check.

    Returns:
        True if file is a spike file, False otherwise.
    """
    return 'spike' in filename.lower()


def process_downloaded_files(
    download_dir: Path, telescope: str, channel: str,
    download_root: str, dir_config: dict, db_config: dict
) -> dict:
    """Process downloaded files: validate, move, and register to DB.

    Args:
        download_dir: Directory containing downloaded files.
        telescope: Telescope name ('aia' or 'hmi').
        channel: Channel name.
        download_root: Root download directory.
        dir_config: Directory configuration.
        db_config: Database configuration (None to skip DB registration).

    Returns:
        Dict with processing results.
    """
    import pandas as pd

    root = Path(download_root)

    result = {
        'processed': 0,
        'registered': 0,
        'invalid_file': 0,
        'invalid_header': 0,
        'invalid_data': 0,
    }

    # Get FITS files (skip spike files for AIA)
    fits_files = list(download_dir.glob("*.fits"))
    if telescope == 'aia':
        fits_files = [f for f in fits_files if not is_spike_file(f.name)]

    if not fits_files:
        print("  No files to process")
        return result

    print(f"  Processing {len(fits_files)} files...")

    records = []

    for file_path in fits_files:
        meta = validate_fits(str(file_path), check_quality=False)

        if isinstance(meta, str):
            # Invalid file: move to appropriate error directory
            error_dir = root / dir_config.get(meta, 'invalid_file')
            error_dir.mkdir(parents=True, exist_ok=True)
            target_path = error_dir / file_path.name

            shutil.move(str(file_path), str(target_path))
            result[meta] = result.get(meta, 0) + 1
            result['processed'] += 1
            continue

        # Valid file: move to target directory
        dt = tai_to_utc(meta['datetime'], telescope)
        target_dir = get_target_path(download_root, telescope, dt)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / file_path.name

        shutil.move(str(file_path), str(target_path))
        result['processed'] += 1

        # Prepare DB record
        if db_config:
            records.append({
                'telescope': telescope,
                'channel': channel,
                'datetime': dt,
                'file_path': str(target_path),
                'quality': meta.get('quality'),
                'wavelength': meta.get('wavelength'),
                'exposure_time': None,
            })

    # Register all valid files in DB
    if db_config and records:
        df = pd.DataFrame(records)
        inserted = upsert(df, 'sdo', db_config,
                          conflict_columns=['telescope', 'channel', 'datetime'])
        result['registered'] = inserted
        print(f"  Registered {inserted} files to DB")

    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Download SDO files from URL list JSON'
    )

    # Input options
    parser.add_argument('--input', '-i', type=str, required=True,
                        help='Input JSON file with URL list')

    # Download options
    parser.add_argument('--parallel', type=int, default=4,
                        help='Number of parallel downloads (default: 4)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files')

    # Processing options
    parser.add_argument('--skip-process', action='store_true',
                        help='Skip file processing (only download)')
    parser.add_argument('--skip-db', action='store_true',
                        help='Skip database registration')
    parser.add_argument('--init-db', action='store_true',
                        help='Initialize database tables before processing')

    # Filter options
    parser.add_argument('--channels', type=str, nargs='+', default=None,
                        help='Only download specific channels (default: all)')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    args = parser.parse_args()

    # Read input JSON
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)

    with open(input_path) as f:
        url_data = json.load(f)

    # Parse target time from JSON
    target_time = datetime.datetime.fromisoformat(url_data['target_time'])
    query_time = datetime.datetime.fromisoformat(url_data['query_time'])
    time_range = url_data.get('time_range', 6)
    telescope = url_data.get('telescope', 'aia')

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']

    download_config = config['download_config'].get('sdo', {})
    download_root = download_config.get('download_root', '/opt/archive/sdo')
    dir_config = download_config.get('dirs', {
        'downloaded': 'downloaded',
        'invalid_file': 'invalid_file',
        'invalid_header': 'invalid_header',
        'invalid_data': 'invalid_data',
    })

    # Initialize database if requested
    if args.init_db and not args.skip_db:
        initialize_database(db_config, config['schema_config'])

    # Create download directory
    root = Path(download_root)
    download_dir = root / dir_config['downloaded']
    download_dir.mkdir(parents=True, exist_ok=True)

    # Check query age
    age_hours = (datetime.datetime.now() - query_time).total_seconds() / 3600
    if age_hours > 24:
        print(f"Warning: Query was made {age_hours:.1f} hours ago. URLs may have expired.")
        print()

    print("SDO File Downloader (from URL list)")
    print(f"Input file: {args.input}")
    print(f"Query time: {query_time}")
    print(f"Target time: {target_time}")
    print(f"Telescope: {telescope}")
    print(f"Time range: +/- {time_range} minutes")
    print(f"Download root: {download_root}")
    print(f"Parallel downloads: {args.parallel}")
    print()

    # Get channels from JSON
    channels_data = url_data.get('channels', {})
    channels_to_process = args.channels or list(channels_data.keys())

    total_stats = {
        'downloaded': 0,
        'skipped': 0,
        'failed': 0,
        'registered': 0,
    }

    for channel in channels_to_process:
        if channel not in channels_data:
            print(f"[{telescope.upper()} / {channel}] Not found in URL list, skipping")
            print()
            continue

        channel_data = channels_data[channel]
        files = channel_data.get('files', [])

        print(f"[{telescope.upper()} / {channel}]")
        print(f"  Files in list: {len(files)}")

        if not files:
            print("  No files to download")
            print()
            continue

        # Check DB for existing record in time range
        if not args.skip_db and not args.skip_process:
            if check_db_exists_in_range(
                target_time, time_range, 'sdo', db_config,
                telescope=telescope, channel=channel
            ):
                print(f"  Already exists in DB for {target_time} ± {time_range}min")
                print()
                continue

        # Build download tasks
        download_tasks = []
        skipped = 0

        for file_info in files:
            url = file_info['url']
            filename = file_info['filename']

            # Check if file exists anywhere
            existing = file_exists_anywhere(filename, download_root, dir_config)
            if existing and not args.overwrite:
                skipped += 1
                continue

            save_path = str(download_dir / filename)
            download_tasks.append((url, save_path))

        print(f"  Files to download: {len(download_tasks)}, skipped: {skipped}")
        total_stats['skipped'] += skipped

        # Download files
        if download_tasks:
            print("  Downloading...")
            success, failed = download_files_parallel(
                download_tasks, max_workers=args.parallel, overwrite=args.overwrite
            )
            print(f"  Downloaded: {success}, failed: {failed}")
            total_stats['downloaded'] += success
            total_stats['failed'] += failed

        # Process downloaded files
        if not args.skip_process:
            result = process_downloaded_files(
                download_dir, telescope, channel,
                download_root, dir_config, db_config if not args.skip_db else None
            )

            total_stats['registered'] += result['registered']

            print(f"  Processed: {result['processed']}, "
                  f"registered={result['registered']}, "
                  f"invalid={result['invalid_file']}, "
                  f"invalid_header={result['invalid_header']}, "
                  f"invalid_data={result['invalid_data']}")

        print()

    print("=" * 50)
    print(f"Total downloaded: {total_stats['downloaded']}")
    print(f"Total skipped: {total_stats['skipped']}")
    print(f"Total failed: {total_stats['failed']}")
    print(f"Total registered: {total_stats['registered']}")


if __name__ == '__main__':
    main()

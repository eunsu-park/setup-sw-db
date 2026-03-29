"""Register existing LASCO FITS files to database.

Scans camera directories for FITS files and registers them
in the unified lasco table.
"""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    load_config,
    create_database,
    create_tables,
    upsert,
    delete_orphans,
    get_lasco_record,
)


def scan_fits_files(download_root: str, camera: str) -> list[Path]:
    """Scan for existing FITS files.

    Args:
        download_root: Root directory of downloads.
        camera: Camera name (c1, c2, c3, c4).

    Returns:
        List of FITS file paths.
    """
    camera_dir = Path(download_root) / camera
    if not camera_dir.exists():
        return []

    return sorted(camera_dir.rglob("*.fts"))


def process_files(files: list[Path], camera: str, db_config: dict,
                  batch_size: int = 1000,
                  verbose: bool = False) -> tuple[int, int]:
    """Parse FITS files and insert to database.

    Args:
        files: List of FITS file paths.
        camera: Camera name.
        db_config: Database configuration.
        batch_size: Number of records per batch insert.
        verbose: If True, print parse details.

    Returns:
        Tuple of (inserted_count, failed_count).
    """
    import pandas as pd

    records = []
    total_inserted = 0
    failed_count = 0

    for i, file_path in enumerate(files):
        record = get_lasco_record(str(file_path), camera=camera)

        if record is None:
            failed_count += 1
            if verbose:
                print(f"    Parse failed: {file_path}")
            continue

        records.append(record)

        if verbose:
            print(f"    Parsed: {file_path.name} -> {record['datetime']}")

        # Batch insert
        if len(records) >= batch_size:
            df = pd.DataFrame(records)
            inserted = upsert(df, 'lasco', db_config,
                              conflict_columns=['camera', 'datetime'])
            total_inserted += inserted
            print(f"  Processed {i + 1}/{len(files)} files, inserted {inserted} records")
            records = []

    # Insert remaining records
    if records:
        df = pd.DataFrame(records)
        inserted = upsert(df, 'lasco', db_config,
                          conflict_columns=['camera', 'datetime'])
        total_inserted += inserted

    return total_inserted, failed_count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Register existing LASCO FITS files to database'
    )

    # Camera options
    parser.add_argument('--cameras', type=str, nargs='+', default=['c2'],
                        choices=['c1', 'c2', 'c3', 'c4'],
                        help='LASCO cameras to scan (default: c2)')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    # Database options
    parser.add_argument('--init-db', action='store_true',
                        help='Initialize database and tables')

    # Batch size
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for DB insert (default: 1000)')

    # Debug options
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print parse failures')
    parser.add_argument('--check-first', type=int, default=0,
                        help='Only check first N files for debugging')

    # Cleanup options
    parser.add_argument('--clean-orphans', action='store_true',
                        help='Delete DB records where file no longer exists')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']

    # Get download root
    download_config = config['download_config'].get('lasco', {})
    download_root = download_config.get('download_root', '/opt/archive/solar_images/lasco')

    # Initialize database if requested
    if args.init_db:
        print("[Database Initialization]")
        create_database(db_config)
        create_tables(db_config, config['schema_config'])
        print()

    # Clean orphan records if requested
    if args.clean_orphans:
        print("[Cleaning Orphan Records]")
        deleted = delete_orphans('lasco', db_config)
        print(f"  {deleted} orphan records deleted")
        print()

    print("LASCO FITS File Registration")
    print(f"Download root: {download_root}")
    print(f"Cameras: {args.cameras}")
    print()

    # Process each camera
    total_files = 0
    total_inserted = 0
    total_failed = 0

    for camera in args.cameras:
        print(f"[{camera.upper()}]")

        files = scan_fits_files(download_root, camera)
        print(f"  Found {len(files)} FITS files")

        if args.check_first > 0:
            files = files[:args.check_first]
            print(f"  Checking first {len(files)} files only")

        if files:
            inserted, failed = process_files(
                files, camera, db_config, args.batch_size, args.verbose
            )
            print(f"  Inserted {inserted} records, {failed} parse failures")
            total_files += len(files)
            total_inserted += inserted
            total_failed += failed

        print()

    print(f"Total: {total_files} files scanned, {total_inserted} inserted, {total_failed} failed")


if __name__ == '__main__':
    main()

"""Scan, validate, classify, and register SDO FITS files to database.

Recursively scans a directory for FITS files, auto-detects telescope/channel
from FITS headers, moves files to the correct directory structure,
and registers valid files in the unified sdo table.

Directory structure after processing:
    {download_root}/
        aia/{YYYY}/{YYYYMMDD}/   <- valid AIA files
        hmi/{YYYY}/{YYYYMMDD}/   <- valid HMI files
        invalid_file/            <- corrupted or unreadable
        invalid_header/          <- missing required headers
        invalid_data/            <- data errors
"""
import argparse
import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    load_config,
    create_database,
    create_tables,
    upsert,
    delete_orphans,
    validate_fits,
    tai_to_utc,
    get_target_path,
)


def scan_fits_files(scan_dir: str) -> list[Path]:
    """Recursively scan directory for FITS files.

    Args:
        scan_dir: Directory to scan.

    Returns:
        Sorted list of FITS file paths.
    """
    scan_path = Path(scan_dir)
    if not scan_path.exists():
        print(f"  Directory not found: {scan_dir}")
        return []

    files = sorted(scan_path.rglob("*.fits"))

    # Filter out spike files (AIA artifact)
    files = [f for f in files if 'spike' not in f.name.lower()]

    return files


def _validate_single(file_path_str: str) -> tuple[str, dict | str]:
    """Validate a single FITS file (worker function for parallel execution).

    Args:
        file_path_str: Path to FITS file as string.

    Returns:
        Tuple of (file_path_str, validation_result).
    """
    result = validate_fits(file_path_str, check_quality=False, check_data=False)
    return (file_path_str, result)


def _validate_batch(files: list[Path], parallel: int) -> list[tuple[str, dict | str]]:
    """Validate a batch of FITS files.

    Uses ProcessPoolExecutor when parallel > 1, otherwise validates sequentially.

    Args:
        files: List of FITS file paths.
        parallel: Number of parallel workers.

    Returns:
        List of (file_path_str, validation_result) tuples in original order.
    """
    file_strs = [str(f) for f in files]

    if parallel > 1:
        from concurrent.futures import ProcessPoolExecutor

        chunksize = max(1, len(file_strs) // (parallel * 4))
        with ProcessPoolExecutor(max_workers=parallel) as executor:
            results = list(executor.map(_validate_single, file_strs,
                                        chunksize=chunksize))
    else:
        results = [_validate_single(f) for f in file_strs]

    return results


def _process_batch(validation_results: list[tuple[str, dict | str]],
                   download_root: str, dir_config: dict, db_config: dict,
                   move: bool, verbose: bool, counts: dict) -> int:
    """Process a validated batch: insert to DB, then move files.

    Args:
        validation_results: List of (file_path_str, validation_result) tuples.
        download_root: Root directory for file storage.
        dir_config: Directory configuration for error subdirs.
        db_config: Database configuration.
        move: If True, move files to target directories.
        verbose: If True, print details for each file.
        counts: Shared counts dict to update in place.

    Returns:
        Number of records inserted.
    """
    import pandas as pd

    root = Path(download_root)
    records = []
    valid_moves = []  # (source, target) pairs to move after DB insert
    invalid_moves = []  # (source, target) pairs for invalid files

    for file_path_str, result in validation_results:
        file_path = Path(file_path_str)

        if isinstance(result, str):
            counts[result] = counts.get(result, 0) + 1

            if move and result in dir_config:
                target_dir = root / dir_config[result]
                target_path = target_dir / file_path.name
                if target_path != file_path:
                    invalid_moves.append((file_path, target_path))

            if verbose:
                print(f"    {result}: {file_path.name}")
            continue

        telescope = result.get('telescope')
        channel = result.get('channel')
        dt = tai_to_utc(result['datetime'], telescope)

        if move:
            target_dir = get_target_path(download_root, telescope, dt)
            target_path = target_dir / file_path.name

            if target_path == file_path:
                pass
            elif target_path.exists():
                counts['skipped'] += 1
                if verbose:
                    print(f"    skipped (exists): {file_path.name}")
                continue
            else:
                valid_moves.append((file_path, target_path))
        else:
            target_path = file_path

        records.append({
            'telescope': telescope,
            'channel': channel,
            'datetime': dt,
            'file_path': str(target_path),
            'quality': result.get('quality'),
            'wavelength': result.get('wavelength'),
            'exposure_time': None,
        })

        counts['valid'] += 1

        if verbose:
            print(f"    valid: {file_path.name} -> {telescope}/{channel} {dt}")

    # Step 1: DB insert
    inserted = 0
    if records:
        df = pd.DataFrame(records)
        inserted = upsert(df, 'sdo', db_config,
                          conflict_columns=['telescope', 'channel', 'datetime'])

    # Step 2: Move valid files after successful DB insert
    for source, target in valid_moves:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))

    # Step 3: Move invalid files
    for source, target in invalid_moves:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))

    return inserted


def process_files(files: list[Path], download_root: str, dir_config: dict,
                  db_config: dict, batch_size: int = 1000,
                  move: bool = True, parallel: int = 1,
                  verbose: bool = False) -> dict:
    """Validate, register, and move FITS files in batches.

    Each batch is processed as: validate -> DB upsert -> move files.
    This ensures progress is visible during processing and files are only
    moved after successful DB insertion.

    Args:
        files: List of FITS file paths to process.
        download_root: Root directory for file storage.
        dir_config: Directory configuration for error subdirs.
        db_config: Database configuration.
        batch_size: Number of files per batch.
        move: If True, move files to target directories.
        parallel: Number of parallel validation workers.
        verbose: If True, print details for each file.

    Returns:
        Dict with counts: valid, invalid_file, invalid_header, invalid_data, skipped.
    """
    total = len(files)
    counts = {
        'valid': 0,
        'invalid_file': 0,
        'invalid_header': 0,
        'invalid_data': 0,
        'skipped': 0,
    }
    total_inserted = 0

    for start in range(0, total, batch_size):
        batch = files[start:start + batch_size]
        end = min(start + len(batch), total)

        # Step 1: Validate batch
        validation_results = _validate_batch(batch, parallel)

        # Step 2: DB insert, then move files
        inserted = _process_batch(
            validation_results, download_root, dir_config, db_config,
            move, verbose, counts,
        )
        total_inserted += inserted

        print(f"  Batch {start + 1}-{end}/{total}: "
              f"inserted {inserted}, "
              f"valid {counts['valid']}, "
              f"invalid {counts['invalid_file'] + counts['invalid_header'] + counts['invalid_data']}, "
              f"skipped {counts['skipped']}",
              flush=True)

    counts['db_inserted'] = total_inserted
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Scan, classify, and register SDO FITS files to database'
    )

    # Scan directory
    parser.add_argument('scan_dir', type=str, nargs='?', default=None,
                        help='Directory to scan for FITS files '
                             '(default: {download_root}/downloaded)')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    # Database options
    parser.add_argument('--init-db', action='store_true',
                        help='Initialize database and tables')

    # Processing options
    parser.add_argument('--no-move', action='store_true',
                        help='Register only, do not move files')
    parser.add_argument('--parallel', type=int, default=4,
                        help='Number of parallel validation workers (default: 4)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for DB insert (default: 1000)')

    # Debug options
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print details for each file')
    parser.add_argument('--check-first', type=int, default=0,
                        help='Only process first N files for debugging')

    # Cleanup options
    parser.add_argument('--clean-orphans', action='store_true',
                        help='Delete DB records where file no longer exists')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']

    # Get download config
    download_config = config['download_config'].get('sdo', {})
    download_root = download_config.get('download_root', '/opt/nas/archive/sdo')
    dir_config = download_config.get('dirs', {
        'aia': 'aia',
        'hmi': 'hmi',
        'downloaded': 'downloaded',
        'invalid_file': 'invalid_file',
        'invalid_header': 'invalid_header',
        'invalid_data': 'invalid_data',
    })

    # Default scan directory
    scan_dir = args.scan_dir or str(Path(download_root) / dir_config['downloaded'])

    # Initialize database if requested
    if args.init_db:
        print("[Database Initialization]")
        create_database(db_config)
        create_tables(db_config, config['schema_config'])
        print()

    # Clean orphan records if requested
    if args.clean_orphans:
        print("[Cleaning Orphan Records]")
        deleted = delete_orphans('sdo', db_config)
        print(f"  {deleted} orphan records deleted")
        print()
        if not args.scan_dir:
            return

    # Scan for FITS files
    print("SDO FITS File Registration")
    print(f"Scan directory: {scan_dir}")
    print(f"Download root: {download_root}")
    print(f"Move files: {not args.no_move}")
    print(f"Parallel workers: {args.parallel}")
    print()

    files = scan_fits_files(scan_dir)
    print(f"Found {len(files)} FITS files")

    if not files:
        return

    if args.check_first > 0:
        files = files[:args.check_first]
        print(f"Processing first {len(files)} files only")

    print()

    # Process files
    counts = process_files(
        files, download_root, dir_config, db_config,
        batch_size=args.batch_size,
        move=not args.no_move,
        parallel=args.parallel,
        verbose=args.verbose,
    )

    # Summary
    print()
    print("=" * 50)
    print(f"Valid: {counts['valid']}")
    print(f"Invalid file: {counts['invalid_file']}")
    print(f"Invalid header: {counts['invalid_header']}")
    print(f"Invalid data: {counts['invalid_data']}")
    print(f"Skipped (exists): {counts['skipped']}")
    print(f"DB inserted: {counts['db_inserted']}")


if __name__ == '__main__':
    main()

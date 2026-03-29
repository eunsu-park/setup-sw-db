"""LASCO-specific functions for VSO query and download."""
import datetime
from pathlib import Path


def query_vso_lasco(camera: str, start_date: datetime.date,
                    end_date: datetime.date) -> list:
    """Query VSO for LASCO data using SunPy Fido.

    Args:
        camera: LASCO camera name (c1, c2, c3).
        start_date: Start date.
        end_date: End date.

    Returns:
        Fido search results.
    """
    from sunpy.net import Fido, attrs as a

    # LASCO detector mapping
    detector_map = {
        'c1': 'C1',
        'c2': 'C2',
        'c3': 'C3',
    }

    detector = detector_map.get(camera.lower())
    if not detector:
        print(f"  Unknown camera: {camera}")
        return []

    # Build time range (include full end_date)
    start_time = datetime.datetime.combine(start_date, datetime.time.min)
    end_time = datetime.datetime.combine(end_date, datetime.time(23, 59, 59))

    print(f"  Querying VSO for LASCO {detector}...")
    print(f"  Time range: {start_time} to {end_time}")

    try:
        results = Fido.search(
            a.Time(start_time, end_time),
            a.Instrument('LASCO'),
            a.Detector(detector),
        )

        if len(results) == 0:
            print("  No records found")
            return []

        total_files = sum(len(r) for r in results)
        print(f"  Found {total_files} records")

        return results

    except Exception as e:
        print(f"  VSO query error: {e}")
        return []


def download_vso_lasco(results, download_dir: str, camera: str,
                       overwrite: bool = False,
                       existing_files: set[str] = None) -> tuple[int, int, list[Path]]:
    """Download LASCO files from VSO query results.

    Args:
        results: Fido search results.
        download_dir: Directory to save downloaded files.
        camera: Camera name for subdirectory organization.
        overwrite: If True, overwrite existing files.
        existing_files: Set of filenames already in local directory (skip these).

    Returns:
        Tuple of (downloaded_count, skipped_count, list of downloaded file paths).
    """
    from sunpy.net import Fido

    if not results or len(results) == 0:
        return 0, 0, []

    existing_files = existing_files or set()

    # Filter results to exclude existing files (unless overwrite)
    if existing_files and not overwrite:
        vso_filenames = get_vso_filenames(results)
        to_skip = vso_filenames & existing_files
        skipped = len(to_skip)

        if skipped > 0:
            print(f"  Skipping {skipped} existing files")

        # If all files exist, skip download
        if skipped == len(vso_filenames):
            return 0, skipped, []
    else:
        skipped = 0

    download_path = Path(download_dir)
    download_path.mkdir(parents=True, exist_ok=True)

    print(f"  Downloading to {download_path}...")

    try:
        # Download files
        downloaded = Fido.fetch(
            results,
            path=str(download_path / '{file}'),
            overwrite=overwrite,
        )

        # Get list of downloaded file paths
        file_paths = [Path(f) for f in downloaded]
        valid_paths = [p for p in file_paths if p.exists()]

        # Filter out files that were already existing (Fido may return them)
        if existing_files and not overwrite:
            new_paths = [p for p in valid_paths if p.name not in existing_files]
        else:
            new_paths = valid_paths

        print(f"  Downloaded {len(new_paths)} files")

        return len(new_paths), skipped, new_paths

    except Exception as e:
        print(f"  Download error: {e}")
        return 0, skipped, []


def get_lasco_save_dir(download_root: str, camera: str,
                       dt: datetime.datetime | datetime.date) -> Path:
    """Get target save directory for LASCO file.

    Args:
        download_root: Root download directory.
        camera: Camera name (c1, c2, c3).
        dt: Observation datetime or date.

    Returns:
        Target directory path.
    """
    return (Path(download_root) / camera / f"{dt.year:04d}" /
            f"{dt.year:04d}{dt.month:02d}{dt.day:02d}")


def get_vso_filenames(results) -> set[str]:
    """Extract filenames from VSO query results.

    Args:
        results: Fido search results.

    Returns:
        Set of filenames.
    """
    if not results or len(results) == 0:
        return set()

    filenames = set()
    for table in results:
        if hasattr(table, 'colnames') and 'fileid' in table.colnames:
            for row in table:
                # fileid format: /soho/private/data/processed/lasco/level_05/960103/c2/22000146.fts
                fileid = str(row['fileid'])
                filename = fileid.split('/')[-1]
                filenames.add(filename)
    return filenames


def lasco_file_exists(filename: str, download_root: str, camera: str) -> str | None:
    """Check if LASCO file exists anywhere under camera directory.

    Searches recursively: {download_root}/{camera}/**/{filename}

    Args:
        filename: Filename to check (e.g., '22000146.fts').
        download_root: Root download directory.
        camera: Camera name (c1, c2, c3).

    Returns:
        Full path string if file exists, None otherwise.
    """
    camera_dir = Path(download_root) / camera

    if not camera_dir.exists():
        return None

    # Search recursively under camera directory
    matches = list(camera_dir.rglob(filename))
    if matches:
        return str(matches[0])

    return None


def extract_lasco_metadata(file_path: str) -> dict | None:
    """Extract metadata from LASCO FITS file.

    Args:
        file_path: Path to FITS file.

    Returns:
        Dict with metadata (datetime, camera, exposure_time, filter) or None.
    """
    from .parse import parse_fits_datetime

    try:
        from astropy.io import fits
        with fits.open(file_path) as hdul:
            header = hdul[0].header

            # Get datetime
            dt = parse_fits_datetime(file_path)
            if dt is None:
                return None

            # Get camera from detector
            detector = header.get('DETECTOR', '')
            camera = detector.lower() if detector else None

            # Get exposure time
            exposure_time = header.get('EXPTIME') or header.get('EXP_TIME')

            # Get filter
            filter_val = header.get('FILTER') or header.get('FILTER1')

            return {
                'datetime': dt,
                'camera': camera,
                'exposure_time': float(exposure_time) if exposure_time else None,
                'filter': str(filter_val) if filter_val else None,
            }

    except Exception:
        return None


def get_lasco_record(file_path: str, camera: str = None) -> dict | None:
    """Create a database record dict for LASCO file.

    Args:
        file_path: Path to FITS file.
        camera: Camera name (if not provided, extracted from file).

    Returns:
        Dict ready for database insertion or None.
    """
    metadata = extract_lasco_metadata(file_path)
    if metadata is None:
        return None

    return {
        'camera': camera or metadata.get('camera'),
        'datetime': metadata['datetime'],
        'file_path': str(file_path),
        'exposure_time': metadata.get('exposure_time'),
        'filter': metadata.get('filter'),
    }

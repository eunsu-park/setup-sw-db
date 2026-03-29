"""SECCHI-specific functions for data download and metadata extraction."""
import datetime
from pathlib import Path
from typing import Any


def get_secchi_save_dir(download_root: str, datatype: str, spacecraft: str,
                        instrument: str,
                        dt: datetime.datetime | datetime.date) -> Path:
    """Get target save directory for SECCHI file.

    Args:
        download_root: Root download directory.
        datatype: Data type ('science' or 'beacon').
        spacecraft: Spacecraft name ('ahead' or 'behind').
        instrument: Instrument name ('cor1', 'cor2', 'euvi', 'hi_1', 'hi_2').
        dt: Observation datetime or date.

    Returns:
        Target directory path.
    """
    return (Path(download_root) / datatype / spacecraft /
            instrument / f"{dt.year:04d}" /
            f"{dt.year:04d}{dt.month:02d}{dt.day:02d}")


def extract_secchi_metadata(file_path: str) -> dict[str, Any] | None:
    """Extract metadata from SECCHI FITS file.

    Args:
        file_path: Path to FITS file.

    Returns:
        Dict with metadata or None if extraction fails.
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

            # Get instrument
            detector = header.get('DETECTOR', '').lower()

            # Get exposure time
            exposure_time = header.get('EXPTIME') or header.get('EXP_TIME')

            # Get filter
            filter_val = header.get('FILTER') or header.get('FILTER1')

            # Get wavelength (for EUVI)
            wavelength = header.get('WAVELNTH')

            # Determine channel for EUVI (based on wavelength)
            channel = None
            if detector == 'euvi' and wavelength:
                channel = str(int(wavelength))

            return {
                'datetime': dt,
                'instrument': detector,
                'channel': channel,
                'wavelength': int(wavelength) if wavelength else None,
                'exposure_time': float(exposure_time) if exposure_time else None,
                'filter': str(filter_val) if filter_val else None,
            }

    except Exception:
        return None


def get_secchi_record(file_path: str, datatype: str, spacecraft: str,
                      instrument: str = None) -> dict | None:
    """Create a database record dict for SECCHI file.

    Args:
        file_path: Path to FITS file.
        datatype: Data type ('science' or 'beacon').
        spacecraft: Spacecraft name ('ahead' or 'behind').
        instrument: Instrument name (if not provided, extracted from file).

    Returns:
        Dict ready for database insertion or None.
    """
    metadata = extract_secchi_metadata(file_path)
    if metadata is None:
        return None

    return {
        'datatype': datatype,
        'spacecraft': spacecraft,
        'instrument': instrument or metadata.get('instrument'),
        'channel': metadata.get('channel'),
        'datetime': metadata['datetime'],
        'file_path': str(file_path),
        'exposure_time': metadata.get('exposure_time'),
        'filter': metadata.get('filter'),
        'wavelength': metadata.get('wavelength'),
    }



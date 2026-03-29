"""SDO-specific functions for JSOC query and FITS validation."""
import datetime
from pathlib import Path
from typing import Any

import drms

# TAI-UTC offset in seconds (as of 2017, leap seconds)
TAI_UTC_OFFSET = 37


def tai_to_utc(dt: datetime.datetime, telescope: str) -> datetime.datetime:
    """Convert TAI time to UTC for HMI instruments.

    HMI T_REC is in TAI (International Atomic Time), which is ahead of UTC
    by ~37 seconds (leap seconds). AIA uses UTC, so no conversion needed.

    Args:
        dt: Datetime to convert.
        telescope: Telescope name ('aia' or 'hmi').

    Returns:
        Converted datetime (UTC).
    """
    if telescope == 'hmi':
        return dt - datetime.timedelta(seconds=TAI_UTC_OFFSET)
    return dt


def utc_to_tai(dt: datetime.datetime, telescope: str) -> datetime.datetime:
    """Convert UTC time to TAI for HMI query.

    Args:
        dt: Datetime in UTC.
        telescope: Telescope name ('aia' or 'hmi').

    Returns:
        Converted datetime (TAI for HMI, unchanged for AIA).
    """
    if telescope == 'hmi':
        return dt + datetime.timedelta(seconds=TAI_UTC_OFFSET)
    return dt


def get_jsoc_series(telescope: str, channel: str, jsoc_config: dict) -> str | None:
    """Get JSOC series name for telescope and channel.

    Args:
        telescope: Telescope name ('aia' or 'hmi').
        channel: Channel name (e.g., '193', 'm_45s').
        jsoc_config: JSOC configuration dict.

    Returns:
        JSOC series name or None if not found.
    """
    telescopes = jsoc_config.get('telescopes', {})
    tel_config = telescopes.get(telescope, {})

    if telescope == 'aia':
        return tel_config.get('series')
    elif telescope == 'hmi':
        series_map = tel_config.get('series_map', {})
        return series_map.get(channel)

    return None


def get_wavelength_for_channel(telescope: str, channel: str) -> int | None:
    """Get wavelength for a channel.

    Args:
        telescope: Telescope name.
        channel: Channel name.

    Returns:
        Wavelength in Angstrom or None for HMI.
    """
    if telescope == 'aia':
        try:
            return int(channel)
        except ValueError:
            return None
    return None


def query_jsoc_v2(telescope: str, channel: str, start_date: datetime.date,
                  end_date: datetime.date, cadence: str,
                  jsoc_config: dict) -> list[dict]:
    """Query JSOC for SDO data URLs using telescope + channel.

    Args:
        telescope: Telescope name ('aia' or 'hmi').
        channel: Channel name (e.g., '193', '211', 'm_45s').
        start_date: Start date.
        end_date: End date.
        cadence: Data cadence (e.g., '1h', '12m').
        jsoc_config: JSOC configuration dict.

    Returns:
        List of dicts with 'url', 'filename', 'telescope', 'channel' keys.
    """
    series = get_jsoc_series(telescope, channel, jsoc_config)
    wavelength = get_wavelength_for_channel(telescope, channel)
    email = jsoc_config.get('email')

    if not series:
        print(f"  Unknown telescope/channel: {telescope}/{channel}")
        return []

    if not email:
        print("  Error: JSOC email not configured in jsoc_config")
        return []

    # Build query string
    duration = (end_date - start_date).days + 1
    date_str = start_date.strftime('%Y.%m.%d')

    if wavelength:
        query = f"{series}[{date_str}/{duration}d@{cadence}][{wavelength}]"
    else:
        query = f"{series}[{date_str}/{duration}d@{cadence}]"

    print(f"  JSOC Query: {query}")

    try:
        client = drms.Client(email=email)
        keys = client.query(query, key=['T_REC', 'QUALITY'])

        if keys is None or len(keys) == 0:
            print("  No records found")
            return []

        print(f"  Found {len(keys)} records")

        export = client.export(query, method='url', protocol='fits')
        export.wait()

        if export.status != 0:
            print(f"  Export failed with status {export.status}")
            return []

        results = []
        for i, row in export.urls.iterrows():
            url = row['url']
            filename = url.split('/')[-1] if '/' in url else url
            results.append({
                'url': url,
                'filename': filename,
                'telescope': telescope,
                'channel': channel,
            })

        return results

    except Exception as e:
        print(f"  JSOC query error: {e}")
        return []


def parse_instrument(instrument: str) -> tuple[str, str]:
    """Parse instrument string to telescope and channel.

    Args:
        instrument: Instrument string (e.g., 'aia_193', 'hmi_m_45s').

    Returns:
        Tuple of (telescope, channel).
    """
    if instrument.startswith('aia_'):
        return ('aia', instrument.replace('aia_', ''))
    elif instrument.startswith('hmi_'):
        suffix = instrument.replace('hmi_', '')
        if suffix == 'magnetogram':
            return ('hmi', 'm_45s')
        return ('hmi', suffix)
    return (instrument, '')


def query_jsoc_time_range(instrument: str, target_time: datetime.datetime,
                          minutes: int, jsoc_config: dict) -> list[dict]:
    """Query JSOC for files within +/- minutes of target_time.

    Args:
        instrument: Instrument string (e.g., 'aia_193', 'hmi_m_45s').
        target_time: Target time (UTC).
        minutes: Search range in minutes (+/-).
        jsoc_config: JSOC configuration.

    Returns:
        List of dicts with 'url', 'filename', and 't_rec' keys.
    """
    telescope, channel = parse_instrument(instrument)
    series = get_jsoc_series(telescope, channel, jsoc_config)
    wavelength = get_wavelength_for_channel(telescope, channel)
    email = jsoc_config.get('email')

    if not series or not email:
        return []

    # Convert target time to TAI for HMI
    query_time = utc_to_tai(target_time, telescope)

    # Build time range query
    start_time = query_time - datetime.timedelta(minutes=minutes)
    duration_minutes = minutes * 2

    time_str = start_time.strftime('%Y.%m.%d_%H:%M:%S')

    if wavelength:
        query = f"{series}[{time_str}/{duration_minutes}m][{wavelength}]"
    else:
        query = f"{series}[{time_str}/{duration_minutes}m]"

    print(f"  JSOC Query: {query}")

    try:
        client = drms.Client(email=email)
        keys = client.query(query, key=['T_REC', 'QUALITY'])

        if keys is None or len(keys) == 0:
            print("  No records found")
            return []

        print(f"  Found {len(keys)} records")

        # Export request
        export = client.export(query, method='url', protocol='fits')
        export.wait()

        if export.status != 0:
            print(f"  Export failed with status {export.status}")
            return []

        results = []
        for i, row in export.urls.iterrows():
            url = row['url']
            filename = url.split('/')[-1] if '/' in url else url

            # Get T_REC from keys if available
            t_rec = None
            if i < len(keys):
                t_rec = keys.iloc[i]['T_REC'] if 'T_REC' in keys.columns else None

            results.append({
                'url': url,
                'filename': filename,
                't_rec': t_rec,
            })

        return results

    except Exception as e:
        print(f"  JSOC query error: {e}")
        return []


def validate_fits(file_path: str, check_quality: bool = True,
                  check_data: bool = True) -> dict[str, Any] | str:
    """Validate SDO FITS file and extract metadata.

    Args:
        file_path: Path to FITS file.
        check_quality: If True, return 'non_zero_quality' for quality != 0.
                       If False, include all files regardless of quality.
        check_data: If True, decompress and validate pixel data.
                    If False, only validate headers (much faster).

    Returns:
        On success: dict with keys 'datetime', 'quality', 'telescope',
            'channel', 'wavelength', 'content', 't_rec_raw'.
        On failure: one of the following error strings, used by callers
            to route files into corresponding directories:
            - 'invalid_file': FITS file cannot be opened.
            - 'invalid_header': Required header keywords missing.
            - 'invalid_data': Pixel data validation failed.
            - 'non_zero_quality': QUALITY flag is non-zero (only when
              check_quality=True).
    """
    from astropy.io import fits

    try:
        hdul = fits.open(file_path)
    except Exception:
        return 'invalid_file'

    try:
        # SDO FITS: compressed data in HDU 1 (CompImageHDU)
        if len(hdul) > 1 and hdul[1].header.get('T_REC'):
            header = hdul[1].header
            data_hdu = hdul[1]
        else:
            header = hdul[0].header
            data_hdu = hdul[0]

        # Required headers
        t_rec = header.get('T_REC') or header.get('t_rec')
        quality = header.get('QUALITY') or header.get('quality')
        telescop = header.get('TELESCOP') or header.get('telescop')

        if t_rec is None or quality is None or telescop is None:
            return 'invalid_header'

        # Parse datetime from T_REC
        dt = _parse_t_rec(t_rec)
        if dt is None:
            return 'invalid_header'

        # Check data validity (expensive: decompresses Rice-compressed data)
        if check_data:
            import numpy as np
            try:
                data = data_hdu.data
                if data is None or data.size == 0:
                    return 'invalid_data'
                if np.all(np.isnan(data)):
                    return 'invalid_data'
            except Exception:
                return 'invalid_data'

        # Check quality (only if check_quality is True)
        if check_quality and quality != 0:
            return 'non_zero_quality'

        # Get wavelength or content
        wavelength = header.get('WAVELNTH') or header.get('wavelnth')
        content = header.get('CONTENT') or header.get('content')

        # Determine telescope (normalized to lowercase)
        telescope = 'aia' if 'AIA' in str(telescop).upper() else 'hmi'

        # Determine channel
        if telescope == 'aia':
            channel = str(int(wavelength)) if wavelength else None
        else:
            # For HMI, derive channel from content or filename
            if content:
                if 'magnetogram' in content.lower():
                    channel = 'm_45s'  # Default
                elif 'continuum' in content.lower():
                    channel = 'ic_45s'
                else:
                    channel = content.lower().replace(' ', '_')
            else:
                channel = 'm_45s'

        return {
            'datetime': dt,
            'quality': quality,
            'telescope': telescope,
            'channel': channel,
            'wavelength': int(wavelength) if wavelength else None,
            'content': content,
            't_rec_raw': t_rec,
        }

    except Exception:
        return 'invalid_header'
    finally:
        hdul.close()


def _parse_t_rec(t_rec: str) -> datetime.datetime | None:
    """Parse T_REC datetime string.

    Supports formats:
    - YYYY.MM.DD_HH:MM:SS_TAI (HMI)
    - YYYY-MM-DDTHH:MM:SS.sssZ (AIA)

    Args:
        t_rec: T_REC string from FITS header.

    Returns:
        Parsed datetime or None.
    """
    if not t_rec:
        return None

    # TAI format: 2024.01.01_00:00:00_TAI
    if '_TAI' in t_rec:
        date_str = t_rec.replace('_TAI', '')
        try:
            return datetime.datetime.strptime(date_str, '%Y.%m.%d_%H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(date_str, '%Y.%m.%d_%H:%M:%S.%f')
        except ValueError:
            pass

    # ISO format: 2024-01-01T00:00:00.000Z
    if 'T' in t_rec:
        date_str = t_rec.replace('Z', '')
        try:
            return datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            pass

    # Underscore format: 2024.01.01_00:00:00
    if '_' in t_rec and '.' in t_rec:
        try:
            return datetime.datetime.strptime(t_rec, '%Y.%m.%d_%H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(t_rec, '%Y.%m.%d_%H:%M:%S.%f')
        except ValueError:
            pass

    return None


def get_target_path(download_root: str, telescope: str,
                    dt: datetime.datetime) -> Path:
    """Get target file path for valid SDO file.

    Args:
        download_root: Root download directory.
        telescope: Telescope name ('aia' or 'hmi').
        dt: Observation datetime.

    Returns:
        Target directory path (without filename).
    """
    year = dt.year
    date_str = dt.strftime('%Y%m%d')

    return Path(download_root) / telescope / str(year) / date_str


def file_exists_anywhere(filename: str, download_root: str,
                         dir_config: dict) -> str | None:
    """Check if file exists in any of the configured directories.

    Args:
        filename: Filename to check.
        download_root: Root download directory.
        dir_config: Directory configuration dict.

    Returns:
        Path string if file exists, None otherwise.
    """
    root = Path(download_root)

    # Check all configured directories
    for dir_name in dir_config.values():
        dir_path = root / dir_name
        if dir_path.exists():
            # Search recursively
            matches = list(dir_path.rglob(filename))
            if matches:
                return str(matches[0])

    return None


def check_db_exists_in_range(
    target_time: datetime.datetime,
    time_range_minutes: int,
    table_name: str,
    db_config: dict,
    telescope: str,
    channel: str,
    require_quality_zero: bool = True
) -> bool:
    """Check if a record exists in DB within the time range.

    Args:
        target_time: Center time for the range.
        time_range_minutes: Range in minutes (+/-).
        table_name: Database table name ('sdo').
        db_config: Database configuration.
        telescope: Telescope name (e.g., 'aia', 'hmi').
        channel: Channel name (e.g., '193', 'm_45s').
        require_quality_zero: If True, only check for quality=0 records.

    Returns:
        True if record exists in range, False otherwise.
    """
    from egghouse.database import PostgresManager

    start_time = target_time - datetime.timedelta(minutes=time_range_minutes)
    end_time = target_time + datetime.timedelta(minutes=time_range_minutes)

    try:
        with PostgresManager(**db_config) as db:
            quality_condition = "AND quality = 0" if require_quality_zero else ""
            result = db.execute(
                f"SELECT 1 FROM {table_name} WHERE telescope = %s "
                f"AND channel = %s AND datetime BETWEEN %s AND %s "
                f"{quality_condition} LIMIT 1",
                (telescope, channel, start_time, end_time),
                fetch=True
            )
            return len(result) > 0
    except Exception:
        return False

"""Data parsing functions for OMNI and FITS files."""
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from astropy.io import fits


def parse(text: str, spec: dict) -> pd.DataFrame:
    """Parse Fortran-format text into a DataFrame.

    Args:
        text: Raw text data.
        spec: Dict with keys 'columns', 'fills', 'fmt', 'dt_func'.
    """
    # Parse Fortran format string into field positions
    fields = []
    pos = 0
    for m in re.finditer(r'(\d*)([IFA])(\d+)', spec['fmt']):
        repeat = int(m.group(1) or 1)
        ftype, width = m.group(2), int(m.group(3))
        for _ in range(repeat):
            fields.append((pos, pos + width, ftype))
            pos += width
    
    # Parse each line using field positions
    rows = []
    for line in text.strip().split('\n'):
        if not line.strip():
            continue
        row = []
        for start, end, ftype in fields:
            try:
                val = line[start:end].strip()
                row.append(float(val) if val else np.nan)
            except (ValueError, IndexError):
                row.append(np.nan)
        rows.append(row)
    
    df = pd.DataFrame(rows, columns=spec['columns'])
    
    # Replace fill values with NaN (cast to float for comparison)
    for idx, fvals in spec['fills'].items():
        if idx < len(df.columns):
            fvals_float = [float(v) for v in fvals]
            df.iloc[:, idx] = df.iloc[:, idx].replace(fvals_float, np.nan)
    
    # Generate datetime column
    df['datetime'] = df.apply(spec['dt_func'], axis=1)
    df = df[['datetime'] + [c for c in df.columns if c != 'datetime']]
    
    return df


# =============================================================================
# OMNI Data Specifications
# =============================================================================
def _dt_lowres(row):
    try:
        return datetime(int(row['Year']), 1, 1) + \
               timedelta(days=int(row['Decimal_Day']) - 1, hours=int(row['Hour']))
    except (ValueError, TypeError):
        return pd.NaT


def _dt_highres(row):
    try:
        return datetime(int(row['Year']), 1, 1) + \
               timedelta(days=int(row['Day']) - 1, hours=int(row['Hour']), minutes=int(row['Minute']))
    except (ValueError, TypeError):
        return pd.NaT


LOWRES = {
    'table': 'omni_low_resolution',
    'fmt': "(2I4,I3,I5,2I3,2I4,14F6.1,F9.0,F6.1,F6.0,2F6.1,F6.3,F6.2,F9.0,F6.1,F6.0,2F6.1,F6.3,2F7.2,F6.1,I3,I4,I6,I5,F10.2,5F9.2,I3,I4,F6.1,F6.1,2I6,F5.1)",
    'columns': [
        'Year', 'Decimal_Day', 'Hour', 'Bartels_Rotation_Number',
        'IMF_SC_ID', 'SW_Plasma_SC_ID', 'IMF_Avg_Points', 'Plasma_Avg_Points',
        'B_Field_Magnitude_Avg_nT', 'B_Magnitude_of_Avg_Field_Vector_nT',
        'B_Lat_Angle_Avg_Field_Vector_deg', 'B_Long_Angle_Avg_Field_Vector_deg',
        'Bx_GSE_GSM_nT', 'By_GSE_nT', 'Bz_GSE_nT', 'By_GSM_nT', 'Bz_GSM_nT',
        'Sigma_B_Magnitude_nT', 'Sigma_B_Vector_nT', 'Sigma_Bx_nT', 'Sigma_By_nT', 'Sigma_Bz_nT',
        'Proton_Temperature_K', 'Proton_Density_n_cm3', 'Plasma_Flow_Speed_km_s',
        'Plasma_Flow_Long_Angle_deg', 'Plasma_Flow_Lat_Angle_deg',
        'Na_Np_Ratio', 'Flow_Pressure_nPa',
        'Sigma_Temperature_K', 'Sigma_Density_n_cm3', 'Sigma_Flow_Speed_km_s',
        'Sigma_Phi_V_deg', 'Sigma_Theta_V_deg', 'Sigma_Na_Np',
        'Electric_Field_mV_m', 'Plasma_Beta', 'Alfven_Mach_Number',
        'Kp_Index', 'Sunspot_Number_R', 'DST_Index_nT', 'AE_Index_nT',
        'Proton_Flux_gt1MeV', 'Proton_Flux_gt2MeV', 'Proton_Flux_gt4MeV',
        'Proton_Flux_gt10MeV', 'Proton_Flux_gt30MeV', 'Proton_Flux_gt60MeV',
        'Flag', 'ap_Index_nT', 'f10_7_Index_sfu', 'PC_N_Index',
        'AL_Index_nT', 'AU_Index_nT', 'Magnetosonic_Mach_Number'
    ],
    'fills': {
        3: [9999], 4: [99], 5: [99], 6: [999], 7: [999],
        8: [999.9], 9: [999.9], 10: [999.9], 11: [999.9],
        12: [999.9], 13: [999.9], 14: [999.9], 15: [999.9], 16: [999.9],
        17: [999.9], 18: [999.9], 19: [999.9], 20: [999.9], 21: [999.9],
        22: [9999999.], 23: [999.9], 24: [9999.], 25: [999.9], 26: [999.9],
        27: [9.999], 28: [99.99], 29: [9999999.], 30: [999.9], 31: [9999.],
        32: [999.9], 33: [999.9], 34: [9.999], 35: [999.99], 36: [999.99],
        37: [999.9], 38: [99], 39: [999], 40: [99999], 41: [9999],
        42: [999999.99], 43: [99999.99], 44: [99999.99],
        45: [99999.99], 46: [99999.99], 47: [99999.99],
        49: [999], 50: [999.9], 51: [999.9], 52: [99999], 53: [99999], 54: [99.9]
    },
    'dt_func': _dt_lowres
}

HIGHRES = {
    'table': 'omni_high_resolution',
    'fmt': "(2I4,4I3,3I4,2I7,F6.2,I7,8F8.2,4F8.1,F7.2,F9.0,F6.2,2F7.2,F6.1,6F8.2,7I6,F7.2,F5.1)",
    'columns': [
        'Year', 'Day', 'Hour', 'Minute',
        'IMF_SC_ID', 'SW_Plasma_SC_ID', 'IMF_Avg_Points', 'Plasma_Avg_Points', 'Percent_Interp',
        'Timeshift_sec', 'RMS_Timeshift', 'RMS_Phase_Front_Normal', 'Time_Between_Obs_sec',
        'B_Magnitude_nT', 'Bx_GSE_nT', 'By_GSE_nT', 'Bz_GSE_nT', 'By_GSM_nT', 'Bz_GSM_nT',
        'RMS_SD_B_Scalar_nT', 'RMS_SD_B_Vector_nT',
        'Flow_Speed_km_s', 'Vx_GSE_km_s', 'Vy_GSE_km_s', 'Vz_GSE_km_s',
        'Proton_Density_n_cc', 'Temperature_K', 'Flow_Pressure_nPa', 'Electric_Field_mV_m',
        'Plasma_Beta', 'Alfven_Mach_Number',
        'SC_X_GSE_Re', 'SC_Y_GSE_Re', 'SC_Z_GSE_Re',
        'BSN_X_GSE_Re', 'BSN_Y_GSE_Re', 'BSN_Z_GSE_Re',
        'AE_Index_nT', 'AL_Index_nT', 'AU_Index_nT',
        'SYM_D_nT', 'SYM_H_nT', 'ASY_D_nT', 'ASY_H_nT',
        'PC_N_Index', 'Magnetosonic_Mach_Number'
    ],
    'fills': {
        4: [99], 5: [99], 6: [999], 7: [999], 8: [999],
        9: [999999], 10: [999999], 11: [99.99], 12: [999999],
        13: [9999.99], 14: [9999.99], 15: [9999.99], 16: [9999.99],
        17: [9999.99], 18: [9999.99], 19: [9999.99], 20: [9999.99],
        21: [99999.9], 22: [99999.9], 23: [99999.9], 24: [99999.9],
        25: [999.99], 26: [9999999.], 27: [99.99], 28: [999.99],
        29: [999.99], 30: [999.9],
        31: [9999.99], 32: [9999.99], 33: [9999.99],
        34: [9999.99], 35: [9999.99], 36: [9999.99],
        37: [99999], 38: [99999], 39: [99999],
        40: [99999], 41: [99999], 42: [99999], 43: [99999],
        44: [999.99], 45: [99.9]
    },
    'dt_func': _dt_highres
}

HIGHRES_5MIN = {
    'table': 'omni_high_resolution_5min',
    'fmt': "(2I4,4I3,3I4,2I7,F6.2,I7,8F8.2,4F8.1,F7.2,F9.0,F6.2,2F7.2,F6.1,6F8.2,7I6,F7.2,F5.1,3F9.2)",
    'columns': [
        'Year', 'Day', 'Hour', 'Minute',
        'IMF_SC_ID', 'SW_Plasma_SC_ID', 'IMF_Avg_Points', 'Plasma_Avg_Points', 'Percent_Interp',
        'Timeshift_sec', 'RMS_Timeshift', 'RMS_Phase_Front_Normal', 'Time_Between_Obs_sec',
        'B_Magnitude_nT', 'Bx_GSE_nT', 'By_GSE_nT', 'Bz_GSE_nT', 'By_GSM_nT', 'Bz_GSM_nT',
        'RMS_SD_B_Scalar_nT', 'RMS_SD_B_Vector_nT',
        'Flow_Speed_km_s', 'Vx_GSE_km_s', 'Vy_GSE_km_s', 'Vz_GSE_km_s',
        'Proton_Density_n_cc', 'Temperature_K', 'Flow_Pressure_nPa', 'Electric_Field_mV_m',
        'Plasma_Beta', 'Alfven_Mach_Number',
        'SC_X_GSE_Re', 'SC_Y_GSE_Re', 'SC_Z_GSE_Re',
        'BSN_X_GSE_Re', 'BSN_Y_GSE_Re', 'BSN_Z_GSE_Re',
        'AE_Index_nT', 'AL_Index_nT', 'AU_Index_nT',
        'SYM_D_nT', 'SYM_H_nT', 'ASY_D_nT', 'ASY_H_nT',
        'PC_N_Index', 'Magnetosonic_Mach_Number',
        'Proton_Flux_gt10MeV', 'Proton_Flux_gt30MeV', 'Proton_Flux_gt60MeV',
    ],
    'fills': {
        4: [99], 5: [99], 6: [999], 7: [999], 8: [999],
        9: [999999], 10: [999999], 11: [99.99], 12: [999999],
        13: [9999.99], 14: [9999.99], 15: [9999.99], 16: [9999.99],
        17: [9999.99], 18: [9999.99], 19: [9999.99], 20: [9999.99],
        21: [99999.9], 22: [99999.9], 23: [99999.9], 24: [99999.9],
        25: [999.99], 26: [9999999.], 27: [99.99], 28: [999.99],
        29: [999.99], 30: [999.9],
        31: [9999.99], 32: [9999.99], 33: [9999.99],
        34: [9999.99], 35: [9999.99], 36: [9999.99],
        37: [99999], 38: [99999], 39: [99999],
        40: [99999], 41: [99999], 42: [99999], 43: [99999],
        44: [999.99], 45: [99.9],
        46: [99999.99], 47: [99999.99], 48: [99999.99],
    },
    'dt_func': _dt_highres
}


# =============================================================================
# FITS Parsing
# =============================================================================
def _parse_tai_datetime(date_str: str) -> datetime | None:
    """Parse TAI format datetime string.

    Handles SDO TAI format: YYYY.MM.DD_HH:MM:SS_TAI
    Note: TAI to UTC conversion is not performed (difference ~35s).

    Args:
        date_str: TAI format datetime string.

    Returns:
        Datetime object or None if parsing failed.
    """
    if not date_str or '_TAI' not in date_str:
        return None

    # Remove _TAI suffix
    date_str = date_str.replace('_TAI', '')

    # Try YYYY.MM.DD_HH:MM:SS format
    try:
        return datetime.strptime(date_str, '%Y.%m.%d_%H:%M:%S')
    except ValueError:
        pass

    # Try YYYY.MM.DD_HH:MM:SS.fff format (with microseconds)
    try:
        return datetime.strptime(date_str, '%Y.%m.%d_%H:%M:%S.%f')
    except ValueError:
        pass

    return None


def _parse_datetime_string(date_str: str) -> datetime | None:
    """Parse datetime from various string formats.

    Args:
        date_str: Date string to parse.

    Returns:
        Datetime object or None if parsing failed.
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # TAI format (SDO HMI/AIA): YYYY.MM.DD_HH:MM:SS_TAI
    if '_TAI' in date_str:
        result = _parse_tai_datetime(date_str)
        if result:
            return result

    # ISO formats with 'T' separator
    if 'T' in date_str:
        # Remove trailing 'Z' or timezone info
        date_str = date_str.rstrip('Z')

        for fmt in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
                    '%Y/%m/%dT%H:%M:%S.%f', '%Y/%m/%dT%H:%M:%S']:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

    # Date + time with space separator
    for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S.%f', '%Y/%m/%d %H:%M:%S',
                '%d/%m/%y %H:%M:%S.%f', '%d/%m/%y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S.%f', '%d/%m/%Y %H:%M:%S']:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Date-only formats
    for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d/%m/%y', '%d/%m/%Y']:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


def _parse_datetime_from_filename(file_path: str) -> datetime | None:
    """Extract datetime from filename patterns.

    Supports:
    - AIA: aia.lev1_euv_12s.2010-09-01T000008Z.193.image_lev1.fits
    - HMI: hmi.m_45s.20100901_000000_TAI.2.magnetogram.fits

    Args:
        file_path: Path to the file.

    Returns:
        Datetime object or None if parsing failed.
    """
    import os
    filename = os.path.basename(file_path)

    # AIA pattern: YYYY-MM-DDTHHMMSSZ
    aia_match = re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})(\d{2})(\d{2})Z', filename)
    if aia_match:
        date_part = aia_match.group(1)
        h, m, s = aia_match.group(2), aia_match.group(3), aia_match.group(4)
        try:
            return datetime.strptime(f"{date_part}T{h}:{m}:{s}", '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            pass

    # HMI pattern: YYYYMMDD_HHMMSS_TAI
    hmi_match = re.search(r'(\d{8})_(\d{6})_TAI', filename)
    if hmi_match:
        date_part = hmi_match.group(1)
        time_part = hmi_match.group(2)
        try:
            return datetime.strptime(f"{date_part}_{time_part}", '%Y%m%d_%H%M%S')
        except ValueError:
            pass

    return None


def parse_fits_datetime(file_path: str) -> datetime | None:
    """Extract observation datetime from FITS header or filename.

    Supports various FITS header formats including:
    - DATE-OBS with ISO format (YYYY-MM-DDTHH:MM:SS)
    - DATE-OBS + TIME-OBS separate fields
    - T_OBS (SDO format)
    - Legacy DD/MM/YY format
    - Fallback to filename parsing for SDO files

    Args:
        file_path: Path to the FITS file.

    Returns:
        Datetime object or None if parsing failed.
    """
    try:
        with fits.open(file_path) as hdul:
            header = hdul[0].header

            # Try T_REC first (SDO standard), then T_OBS
            t_obs = header.get('T_REC') or header.get('T_OBS')
            if t_obs:
                result = _parse_datetime_string(t_obs)
                if result:
                    return result

            # Try DATE-OBS
            date_obs = header.get('DATE-OBS') or header.get('DATE_OBS')
            if date_obs:
                time_obs = header.get('TIME-OBS') or header.get('TIME_OBS')
                if time_obs:
                    combined = f"{date_obs} {time_obs}"
                    result = _parse_datetime_string(combined)
                else:
                    result = _parse_datetime_string(date_obs)
                if result:
                    return result

    except Exception:
        pass

    # Fallback: try to parse from filename
    return _parse_datetime_from_filename(file_path)



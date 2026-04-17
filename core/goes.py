"""GOES (Geostationary Operational Environmental Satellite) data handling.

Listing and parsing of GOES XRS, MAG, and proton flux products from NOAA NCEI.
Supports both legacy (GOES-13/14/15) and GOES-R (16+) satellites with a single
unified schema per instrument.

NetCDF variable names vary between generations and between science-quality
versions. The parsers below resolve names via `_first_var()` which tries a
list of known aliases and returns NaN for anything missing — so a file that
does not carry a given channel simply leaves that column NULL in the DB.
"""
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .download import list_remote_files

# ---------------------------------------------------------------------------
# Satellite classification
# ---------------------------------------------------------------------------

GOES_R_FIRST = 16  # GOES-16 onward is the R series.


def satellite_generation(satellite: int) -> str:
    """Return 'r' for GOES-R series, 'legacy' otherwise.

    Args:
        satellite: GOES satellite number (e.g., 15 or 16).

    Returns:
        'r' if satellite is GOES-R series (>=16), else 'legacy'.
    """
    return "r" if satellite >= GOES_R_FIRST else "legacy"


def is_goes_r(satellite: int) -> bool:
    """Return True if the satellite is in the GOES-R series."""
    return satellite >= GOES_R_FIRST


# ---------------------------------------------------------------------------
# NCEI file discovery
# ---------------------------------------------------------------------------

_DATE_IN_FILENAME = re.compile(r"_d(\d{8})_")


def _month_iter(start_date: date, end_date: date):
    """Yield (year, month) tuples covering [start_date, end_date] inclusive."""
    y, m = start_date.year, start_date.month
    while (y, m) <= (end_date.year, end_date.month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def _extract_date_from_filename(filename: str) -> date | None:
    """Parse 'YYYYMMDD' from an NCEI filename like sci_*_g16_dYYYYMMDD_v*.nc."""
    match = _DATE_IN_FILENAME.search(filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def _url_pattern_for(satellite: int, instrument_config: dict) -> str:
    """Pick the R-series or legacy URL pattern from instrument download config."""
    key = "url_pattern_r" if is_goes_r(satellite) else "url_pattern_legacy"
    pattern = instrument_config.get(key)
    if pattern is None:
        raise KeyError(f"Missing '{key}' in GOES instrument download config")
    return pattern


def list_goes_files(satellite: int, instrument_config: dict,
                    start_date: date, end_date: date) -> list[tuple[str, str]]:
    """List GOES netCDF files in NCEI for a satellite over a date range.

    Walks each (year, month) directory exposed by NCEI and filters files by
    the date embedded in the filename. Out-of-range or wrong-satellite files
    are excluded.

    Args:
        satellite: GOES satellite number (13..19).
        instrument_config: download_config section for the instrument
            (goes_xrs / goes_mag / goes_proton). Must contain the URL
            patterns and file_extension.
        start_date: First date (inclusive).
        end_date: Last date (inclusive).

    Returns:
        List of (url, filename) tuples, sorted by filename.
    """
    pattern = _url_pattern_for(satellite, instrument_config)
    extension = instrument_config.get("file_extension", ".nc")
    sat_tag = f"g{satellite:02d}"

    results: list[tuple[str, str]] = []
    for year, month in _month_iter(start_date, end_date):
        dir_url = pattern.format(NN=f"{satellite:02d}", YYYY=f"{year:04d}",
                                 MM=f"{month:02d}")
        files = list_remote_files(dir_url, extension=extension)
        for filename in files:
            # Skip files for other satellites or index pages.
            if sat_tag not in filename.lower():
                continue
            file_date = _extract_date_from_filename(filename)
            if file_date is None or not (start_date <= file_date <= end_date):
                continue
            results.append((dir_url + filename, filename))

    results.sort(key=lambda t: t[1])
    return results


def get_goes_save_path(save_dir: str, satellite: int, filename: str) -> Path:
    """Compute local save path: {save_dir}/g{NN}/{YYYY}/{filename}.

    Args:
        save_dir: Instrument archive root.
        satellite: Satellite number.
        filename: Just the basename of the netCDF file.

    Returns:
        Absolute Path for the file.
    """
    file_date = _extract_date_from_filename(filename)
    year = file_date.year if file_date else 0
    return Path(save_dir) / f"g{satellite:02d}" / f"{year:04d}" / filename


def get_goes_year_dir(save_dir: str, satellite: int, year: int) -> Path:
    """Directory where files for a (satellite, year) live."""
    return Path(save_dir) / f"g{satellite:02d}" / f"{year:04d}"


# ---------------------------------------------------------------------------
# netCDF parsing helpers
# ---------------------------------------------------------------------------

def _first_var(ds, names: list[str]):
    """Return the first dataset variable whose name matches, else None."""
    for name in names:
        if name in ds.variables:
            return ds[name]
    return None


def _to_utc_naive(series: pd.Series) -> pd.Series:
    """Strip timezone info, yielding a naive UTC timestamp column."""
    if hasattr(series.dtype, "tz") and series.dtype.tz is not None:
        return series.dt.tz_convert("UTC").dt.tz_localize(None)
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def _open_dataset(path: str):
    """Open a netCDF file with xarray, decoding times."""
    import xarray as xr
    return xr.open_dataset(path, decode_times=True)


def _numeric_column(var) -> np.ndarray | None:
    """Return a 1-D float64 numpy array from an xarray variable, or None."""
    if var is None:
        return None
    values = var.values
    if values.ndim == 0:
        return None
    if values.ndim > 1:
        # Some GOES-R products pack multi-channel data in 2-D arrays; flatten
        # only when the second axis has length 1, else reject.
        if values.shape[-1] == 1:
            values = values.reshape(values.shape[0])
        else:
            return None
    return np.asarray(values, dtype=np.float64)


def _integer_flag(var) -> np.ndarray | None:
    """Coerce a flag-like variable to a nullable Int16 numpy array."""
    arr = _numeric_column(var)
    if arr is None:
        return None
    # Keep NaN positions as NaN; caller will insert→NULL.
    return arr


# ---------------------------------------------------------------------------
# XRS parser
# ---------------------------------------------------------------------------

def parse_goes_xrs_netcdf(path: str, satellite: int) -> pd.DataFrame:
    """Parse a GOES XRS L2 netCDF file into the goes_xrs DataFrame schema.

    Handles both legacy (2-channel: a_flux / b_flux) and GOES-R (4-channel:
    a1/a2/b1/b2 plus primary a_flux / b_flux) naming.

    Args:
        path: Path to the netCDF file.
        satellite: Satellite number (used to populate the satellite column
            and the is_goes_r flag).

    Returns:
        DataFrame with columns matching the goes_xrs schema. Empty if the
        file cannot be parsed.
    """
    try:
        ds = _open_dataset(path)
    except Exception as e:
        print(f"  Failed to open {path}: {e}")
        return pd.DataFrame()

    try:
        time_var = _first_var(ds, ["time"])
        if time_var is None:
            return pd.DataFrame()

        times = _to_utc_naive(pd.Series(time_var.values))

        xrs_a = _numeric_column(_first_var(ds, [
            "xrsa_flux", "a_flux", "xrs_a_flux", "xrsa_primary_chan_flux",
        ]))
        xrs_b = _numeric_column(_first_var(ds, [
            "xrsb_flux", "b_flux", "xrs_b_flux", "xrsb_primary_chan_flux",
        ]))

        xrs_a1 = _numeric_column(_first_var(ds, ["xrsa1_flux", "a1_flux"]))
        xrs_a2 = _numeric_column(_first_var(ds, ["xrsa2_flux", "a2_flux"]))
        xrs_b1 = _numeric_column(_first_var(ds, ["xrsb1_flux", "b1_flux"]))
        xrs_b2 = _numeric_column(_first_var(ds, ["xrsb2_flux", "b2_flux"]))

        xrs_a_flag = _integer_flag(_first_var(ds, [
            "xrsa_flags", "a_flags", "xrsa_flag", "a_flag",
        ]))
        xrs_b_flag = _integer_flag(_first_var(ds, [
            "xrsb_flags", "b_flags", "xrsb_flag", "b_flag",
        ]))

        n = len(times)

        def _fill(arr):
            return arr if arr is not None and len(arr) == n else np.full(n, np.nan)

        df = pd.DataFrame({
            "satellite": np.full(n, satellite, dtype=np.int16),
            "datetime": times.values,
            "xrs_a_flux_w_m2": _fill(xrs_a),
            "xrs_b_flux_w_m2": _fill(xrs_b),
            "xrs_a1_flux_w_m2": _fill(xrs_a1),
            "xrs_a2_flux_w_m2": _fill(xrs_a2),
            "xrs_b1_flux_w_m2": _fill(xrs_b1),
            "xrs_b2_flux_w_m2": _fill(xrs_b2),
            "xrs_a_flag": _fill(xrs_a_flag),
            "xrs_b_flag": _fill(xrs_b_flag),
            "is_goes_r": np.full(n, is_goes_r(satellite), dtype=bool),
        })
        return df
    finally:
        ds.close()


# ---------------------------------------------------------------------------
# MAG parser
# ---------------------------------------------------------------------------

def _extract_vector(ds, vector_candidates: list[str],
                    component_candidates: list[list[str]]) -> tuple:
    """Resolve (bx, by, bz) from either a packed 3-vector or component vars.

    Args:
        ds: xarray Dataset.
        vector_candidates: variable names that might hold a (time, 3) array.
        component_candidates: list of 3 lists, each giving aliases for that
            component (x, y, z).

    Returns:
        (bx, by, bz) as np.ndarray or None triples.
    """
    for name in vector_candidates:
        if name not in ds.variables:
            continue
        arr = ds[name].values
        if arr.ndim == 2 and arr.shape[-1] == 3:
            return (
                np.asarray(arr[:, 0], dtype=np.float64),
                np.asarray(arr[:, 1], dtype=np.float64),
                np.asarray(arr[:, 2], dtype=np.float64),
            )

    comps = []
    for aliases in component_candidates:
        comps.append(_numeric_column(_first_var(ds, aliases)))
    return tuple(comps)


def parse_goes_mag_netcdf(path: str, satellite: int) -> pd.DataFrame:
    """Parse a GOES MAG 1-min netCDF file into the goes_mag DataFrame schema.

    Stores Bx/By/Bz in whatever frame the product publishes (EPN for GOES-R,
    HEN/VDH/GSM for legacy) and records the frame name in coord_frame.

    Args:
        path: Path to the netCDF file.
        satellite: Satellite number.

    Returns:
        DataFrame with columns matching the goes_mag schema.
    """
    try:
        ds = _open_dataset(path)
    except Exception as e:
        print(f"  Failed to open {path}: {e}")
        return pd.DataFrame()

    try:
        time_var = _first_var(ds, ["time"])
        if time_var is None:
            return pd.DataFrame()
        times = _to_utc_naive(pd.Series(time_var.values))
        n = len(times)

        # Order of preference: EPN (GOES-R native) > GSE > GSM > HEN/VDH legacy.
        frames = [
            ("EPN", ["b_epn"], [
                ["b_epn_x", "bx_epn"],
                ["b_epn_y", "by_epn"],
                ["b_epn_z", "bz_epn"],
            ]),
            ("GSE", ["b_gse"], [
                ["b_gse_x", "bx_gse"],
                ["b_gse_y", "by_gse"],
                ["b_gse_z", "bz_gse"],
            ]),
            ("GSM", ["b_gsm"], [
                ["b_gsm_x", "bx_gsm", "h_gsm"],
                ["b_gsm_y", "by_gsm", "e_gsm"],
                ["b_gsm_z", "bz_gsm", "n_gsm"],
            ]),
            ("VDH", ["b_vdh"], [
                ["b_vdh_v", "bv_vdh", "hp", "bv"],
                ["b_vdh_d", "bd_vdh", "he", "bd"],
                ["b_vdh_h", "bh_vdh", "hn", "bh"],
            ]),
        ]

        bx = by = bz = None
        frame_used = None
        for frame_name, packed, components in frames:
            bx, by, bz = _extract_vector(ds, packed, components)
            if all(v is not None and len(v) == n for v in (bx, by, bz)):
                frame_used = frame_name
                break

        def _fill(arr):
            return arr if arr is not None and len(arr) == n else np.full(n, np.nan)

        bx_arr = _fill(bx)
        by_arr = _fill(by)
        bz_arr = _fill(bz)

        bt = _numeric_column(_first_var(ds, [
            "b_total", "bt", "btotal", "b_magnitude",
        ]))
        if bt is None or len(bt) != n:
            bt = np.sqrt(bx_arr ** 2 + by_arr ** 2 + bz_arr ** 2)

        mag_flag = _integer_flag(_first_var(ds, [
            "b_flags", "b_flag", "mag_flags", "mag_flag", "DQF", "b_quality",
        ]))

        df = pd.DataFrame({
            "satellite": np.full(n, satellite, dtype=np.int16),
            "datetime": times.values,
            "bx_nt": bx_arr,
            "by_nt": by_arr,
            "bz_nt": bz_arr,
            "bt_nt": bt,
            "coord_frame": np.full(n, frame_used or "UNKNOWN", dtype=object),
            "mag_flag": _fill(mag_flag),
        })
        return df
    finally:
        ds.close()


# ---------------------------------------------------------------------------
# Proton parser
# ---------------------------------------------------------------------------

# Integral energy thresholds we store in goes_proton.
_PROTON_THRESHOLDS_MEV = [1, 5, 10, 30, 50, 60, 100]


def _resolve_proton_column(ds, threshold_mev: int) -> np.ndarray | None:
    """Look up the integral proton flux column for an energy threshold.

    Tries several naming conventions used by SGPS (GOES-R) and EPS/EPEAD
    (legacy).
    """
    candidates = [
        f"proton_flux_gt{threshold_mev}mev",
        f"P_GT_{threshold_mev}",
        f"PGT{threshold_mev}",
        f"p_gt{threshold_mev}",
        f"avgintprotonflux_gt{threshold_mev}mev",
        f"IntegralProtonFlux_gt{threshold_mev}MeV",
    ]
    return _numeric_column(_first_var(ds, candidates))


def _integrate_differential(diff_flux: np.ndarray, lower_kev: np.ndarray,
                            upper_kev: np.ndarray,
                            threshold_mev: float) -> np.ndarray:
    """Derive integral proton flux above a threshold from differential flux.

    Uses the standard partial-channel sum:
        integral(>T) = Σ_c DiffFlux[c] × max(0, Upper[c] − max(T, Lower[c]))

    When the threshold falls inside a channel, only the portion of the channel
    above T contributes. Channels entirely below T contribute 0. Units: if
    DiffFlux is in protons/(cm²·sr·keV·s) and energies in keV, the result is
    protons/(cm²·sr·s) (pfu).

    Args:
        diff_flux: Differential flux array shaped (time, channel).
        lower_kev: 1-D array of channel lower energy bounds in keV.
        upper_kev: 1-D array of channel upper energy bounds in keV.
        threshold_mev: Integral-flux threshold in MeV.

    Returns:
        1-D array of shape (time,) with integral flux; NaN when the row's
        input was all-NaN or when the threshold exceeds all channel upper
        bounds.
    """
    threshold_kev = threshold_mev * 1000.0
    widths = np.maximum(0.0, upper_kev - np.maximum(threshold_kev, lower_kev))
    if widths.sum() == 0:
        return np.full(diff_flux.shape[0], np.nan)
    integral = np.nansum(diff_flux * widths[np.newaxis, :], axis=1)
    all_nan_rows = np.all(np.isnan(diff_flux), axis=1)
    integral[all_nan_rows] = np.nan
    return integral


def parse_goes_proton_netcdf(path: str, satellite: int) -> pd.DataFrame:
    """Parse a GOES proton flux netCDF file.

    Handles two layouts:
      - GOES-R SGPS: AvgDiffProtonFlux (time, sensor, channel) plus
        DiffProtonLowerEnergy / DiffProtonUpperEnergy (sensor, channel).
        Schema threshold columns are derived by integrating the differential
        flux above each threshold (partial-channel sum).
      - Legacy EPS/EPEAD: per-threshold named scalar columns.

    Thresholds above all differential channels' upper bounds (e.g. >500 MeV
    relative to SGPS diff channels that top out at 404 MeV) are left NaN.

    Args:
        path: Path to the netCDF file.
        satellite: Satellite number.

    Returns:
        DataFrame with columns matching the goes_proton schema.
    """
    try:
        ds = _open_dataset(path)
    except Exception as e:
        print(f"  Failed to open {path}: {e}")
        return pd.DataFrame()

    try:
        time_var = _first_var(ds, ["time"])
        if time_var is None:
            return pd.DataFrame()
        times = _to_utc_naive(pd.Series(time_var.values))
        n = len(times)

        def _fill(arr):
            return arr if arr is not None and len(arr) == n else np.full(n, np.nan)

        columns: dict[str, np.ndarray] = {
            "satellite": np.full(n, satellite, dtype=np.int16),
            "datetime": times.values,
        }

        # Try SGPS (GOES-R) differential-integration first.
        diff_var = _first_var(ds, ["AvgDiffProtonFlux"])
        lower_var = _first_var(ds, ["DiffProtonLowerEnergy"])
        upper_var = _first_var(ds, ["DiffProtonUpperEnergy"])
        resolved = False

        if diff_var is not None and lower_var is not None and upper_var is not None:
            diff_arr = np.asarray(diff_var.values, dtype=np.float64)
            lower_arr = np.asarray(lower_var.values, dtype=np.float64)
            upper_arr = np.asarray(upper_var.values, dtype=np.float64)

            # Collapse sensor axis: flux (time, sensor, channel) → (time, ch);
            # energies (sensor, channel) → (channel,).
            if diff_arr.ndim == 3:
                diff_arr = np.nanmean(diff_arr, axis=1)
            if lower_arr.ndim == 2:
                lower_arr = np.nanmean(lower_arr, axis=0)
            if upper_arr.ndim == 2:
                upper_arr = np.nanmean(upper_arr, axis=0)

            if (diff_arr.ndim == 2 and diff_arr.shape[0] == n
                    and lower_arr.ndim == 1 and upper_arr.ndim == 1
                    and diff_arr.shape[1] == lower_arr.shape[0] == upper_arr.shape[0]):
                resolved = True
                for threshold in _PROTON_THRESHOLDS_MEV:
                    integral = _integrate_differential(
                        diff_arr, lower_arr, upper_arr, float(threshold),
                    )
                    columns[f"proton_flux_gt{threshold}mev"] = integral

        # Legacy fallback: per-threshold named scalar columns.
        if not resolved:
            for threshold in _PROTON_THRESHOLDS_MEV:
                columns[f"proton_flux_gt{threshold}mev"] = _fill(
                    _resolve_proton_column(ds, threshold)
                )

        proton_flag = _integer_flag(_first_var(ds, [
            "proton_flags", "proton_flag", "p_flags", "DQF",
            "IntDQFerrSum",
        ]))
        columns["proton_flag"] = _fill(proton_flag)

        return pd.DataFrame(columns)
    finally:
        ds.close()


# ---------------------------------------------------------------------------
# Instrument dispatcher
# ---------------------------------------------------------------------------

INSTRUMENT_PARSERS = {
    "xrs": parse_goes_xrs_netcdf,
    "mag": parse_goes_mag_netcdf,
    "proton": parse_goes_proton_netcdf,
}

INSTRUMENT_TABLES = {
    "xrs": "goes_xrs",
    "mag": "goes_mag",
    "proton": "goes_proton",
}

INSTRUMENT_CONFIG_KEYS = {
    "xrs": "goes_xrs",
    "mag": "goes_mag",
    "proton": "goes_proton",
}


def parse_goes_netcdf(instrument: str, path: str,
                      satellite: int) -> pd.DataFrame:
    """Dispatch to the correct parser for an instrument.

    Args:
        instrument: One of 'xrs', 'mag', 'proton'.
        path: Path to netCDF file.
        satellite: Satellite number.

    Returns:
        Parsed DataFrame.
    """
    parser = INSTRUMENT_PARSERS.get(instrument)
    if parser is None:
        raise ValueError(f"Unknown GOES instrument: {instrument}")
    return parser(path, satellite)

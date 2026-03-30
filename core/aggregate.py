"""Aggregation functions for 30-minute space weather data."""
from datetime import datetime, timedelta
import pandas as pd
from egghouse.database import PostgresManager


# OMNI 1-min column mapping: output name -> source column in omni_high_resolution
OMNI_COLUMN_MAP = {
    'v': 'flow_speed_km_s',
    'np': 'proton_density_n_cc',
    't': 'temperature_k',
    'bx': 'bx_gse_nt',
    'by': 'by_gsm_nt',
    'bz': 'bz_gsm_nt',
    'bt': 'b_magnitude_nt',
}


def aggregate_sw_30min(start_date: datetime, end_date: datetime,
                       db_config: dict) -> pd.DataFrame:
    """Aggregate OMNI 1-min and HPo 30-min data into 30-min cadence.

    For each 30-min window [t, t+30min), computes mean/min/max of OMNI
    variables from 1-min data. HPo ap30/hp30 are directly matched.

    Args:
        start_date: Start datetime (inclusive).
        end_date: End datetime (inclusive, last window starts here).
        db_config: Database configuration dict.

    Returns:
        DataFrame with 30-min aggregated data. All timestamps are preserved;
        NaN values are kept as-is for downstream filtering.
    """
    # Generate 30-min timestamps
    timestamps = pd.date_range(start_date, end_date, freq='30min')
    if len(timestamps) == 0:
        return pd.DataFrame()

    # Query OMNI 1-min data
    source_cols = list(OMNI_COLUMN_MAP.values())
    source_cols_str = ', '.join(source_cols)
    omni_query = f"""
        SELECT datetime, {source_cols_str}
        FROM omni_high_resolution
        WHERE datetime >= %s AND datetime < %s
        ORDER BY datetime
    """
    # Need data up to end_date + 30min for the last window
    query_end = end_date + timedelta(minutes=30)

    with PostgresManager(**db_config) as db:
        omni_rows = db.execute(omni_query, (start_date, query_end), fetch=True)

    # Build OMNI DataFrame
    if omni_rows:
        omni_df = pd.DataFrame(omni_rows)
        omni_df['datetime'] = pd.to_datetime(omni_df['datetime'])
        omni_df = omni_df.set_index('datetime')
    else:
        omni_df = pd.DataFrame(
            index=pd.DatetimeIndex([], name='datetime'),
            columns=source_cols,
        )

    # Resample to 30-min windows with mean/min/max
    agg_dict = {col: ['mean', 'min', 'max'] for col in source_cols}
    omni_agg = omni_df.resample('30min').agg(agg_dict)

    # Flatten multi-level columns: (source_col, agg) -> output_name_agg
    # pandas uses 'mean' but DB schema uses 'avg'
    AGG_RENAME = {'mean': 'avg', 'min': 'min', 'max': 'max'}
    reverse_map = {v: k for k, v in OMNI_COLUMN_MAP.items()}
    flat_columns = []
    for source_col, agg_type in omni_agg.columns:
        var_name = reverse_map[source_col]
        flat_columns.append(f'{var_name}_{AGG_RENAME[agg_type]}')
    omni_agg.columns = flat_columns

    # Reindex to ensure all 30-min timestamps exist
    omni_agg = omni_agg.reindex(timestamps)
    omni_agg.index.name = 'datetime'

    # Query HPo hp30 data
    hpo_query = """
        SELECT datetime, ap30, hp30
        FROM hpo_hp30
        WHERE datetime >= %s AND datetime <= %s
        ORDER BY datetime
    """
    with PostgresManager(**db_config) as db:
        hpo_rows = db.execute(hpo_query, (start_date, end_date), fetch=True)

    if hpo_rows:
        hpo_df = pd.DataFrame(hpo_rows)
        hpo_df['datetime'] = pd.to_datetime(hpo_df['datetime'])
        hpo_df = hpo_df.set_index('datetime')
    else:
        hpo_df = pd.DataFrame(
            index=pd.DatetimeIndex([], name='datetime'),
            columns=['ap30', 'hp30'],
        )

    # Merge OMNI + HPo on datetime
    result = omni_agg.join(hpo_df, how='left')
    result = result.reset_index()

    return result


def extract_event_data(T: datetime, B: int = 5, A: int = 3,
                       db_config: dict = None) -> pd.DataFrame:
    """Extract sw_30min data for an event window around time T.

    Queries the pre-built sw_30min table for the time range
    [T - B days, T + A days - 30min].

    Args:
        T: Reference time.
        B: Number of days before T (default: 5).
        A: Number of days after T (default: 3).
        db_config: Database configuration dict.

    Returns:
        DataFrame with sw_30min records for the event window.
    """
    start = T - timedelta(days=B)
    end = T + timedelta(days=A) - timedelta(minutes=30)

    sql = """
        SELECT * FROM sw_30min
        WHERE datetime >= %s AND datetime <= %s
        ORDER BY datetime
    """

    with PostgresManager(**db_config) as db:
        rows = db.execute(sql, (start, end), fetch=True)

    if rows:
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    return pd.DataFrame()

"""Query functions for solar image data retrieval."""
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
from egghouse.database import PostgresManager


def get_sdo_best_match(
    db_config: dict,
    telescope: str,
    channel: str,
    target_time: datetime,
    time_range_minutes: int = 30,
    require_quality_zero: bool = True
) -> Optional[dict]:
    """Get SDO data closest to target time within time range.

    Args:
        db_config: Database configuration dict.
        telescope: Telescope name ('aia' or 'hmi').
        channel: Channel name (e.g., '193', '211', 'm_45s').
        target_time: Target datetime to find closest match.
        time_range_minutes: Time range in minutes (±) to search.
        require_quality_zero: If True, only return quality=0 data.

    Returns:
        Dict with record data or None if not found.
    """
    quality_condition = "AND quality = 0" if require_quality_zero else ""

    sql = f"""
        SELECT * FROM sdo
        WHERE telescope = %s
          AND channel = %s
          {quality_condition}
          AND datetime BETWEEN %s AND %s
        ORDER BY ABS(EXTRACT(EPOCH FROM (datetime - %s)))
        LIMIT 1
    """

    start_time = target_time - timedelta(minutes=time_range_minutes)
    end_time = target_time + timedelta(minutes=time_range_minutes)

    with PostgresManager(**db_config) as db:
        result = db.execute(
            sql,
            (telescope, channel, start_time, end_time, target_time),
            fetch=True
        )

        if result:
            return result[0]
        return None


def get_sdo_best_matches(
    db_config: dict,
    telescope: str,
    channel: str,
    target_times: list[datetime],
    time_range_minutes: int = 30,
    require_quality_zero: bool = True
) -> pd.DataFrame:
    """Get SDO data closest to multiple target times.

    Args:
        db_config: Database configuration dict.
        telescope: Telescope name ('aia' or 'hmi').
        channel: Channel name (e.g., '193', '211', 'm_45s').
        target_times: List of target datetimes to find closest matches.
        time_range_minutes: Time range in minutes (±) to search.
        require_quality_zero: If True, only return quality=0 data.

    Returns:
        DataFrame with best matches for each target time.
    """
    results = []
    for target_time in target_times:
        match = get_sdo_best_match(
            db_config, telescope, channel, target_time,
            time_range_minutes, require_quality_zero
        )
        if match:
            match['target_time'] = target_time
            results.append(match)

    if results:
        return pd.DataFrame(results)
    return pd.DataFrame()


def get_lasco_data(
    db_config: dict,
    camera: str,
    start_time: datetime,
    end_time: datetime
) -> pd.DataFrame:
    """Get LASCO data within time range.

    Args:
        db_config: Database configuration dict.
        camera: Camera name ('c1', 'c2', 'c3', 'c4').
        start_time: Start datetime.
        end_time: End datetime.

    Returns:
        DataFrame with LASCO records.
    """
    sql = """
        SELECT * FROM lasco
        WHERE camera = %s
          AND datetime BETWEEN %s AND %s
        ORDER BY datetime
    """

    with PostgresManager(**db_config) as db:
        result = db.execute(sql, (camera, start_time, end_time), fetch=True)

        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()


def get_secchi_data(
    db_config: dict,
    datatype: str,
    spacecraft: str,
    instrument: str,
    start_time: datetime,
    end_time: datetime,
    channel: str = None
) -> pd.DataFrame:
    """Get SECCHI data within time range.

    Args:
        db_config: Database configuration dict.
        datatype: Data type ('science' or 'beacon').
        spacecraft: Spacecraft name ('ahead' or 'behind').
        instrument: Instrument name ('cor1', 'cor2', 'euvi', 'hi_1', 'hi_2').
        start_time: Start datetime.
        end_time: End datetime.
        channel: Optional channel for EUVI ('171', '195', '284', '304').

    Returns:
        DataFrame with SECCHI records.
    """
    if channel:
        sql = """
            SELECT * FROM secchi
            WHERE datatype = %s
              AND spacecraft = %s
              AND instrument = %s
              AND channel = %s
              AND datetime BETWEEN %s AND %s
            ORDER BY datetime
        """
        params = (datatype, spacecraft, instrument, channel, start_time, end_time)
    else:
        sql = """
            SELECT * FROM secchi
            WHERE datatype = %s
              AND spacecraft = %s
              AND instrument = %s
              AND datetime BETWEEN %s AND %s
            ORDER BY datetime
        """
        params = (datatype, spacecraft, instrument, start_time, end_time)

    with PostgresManager(**db_config) as db:
        result = db.execute(sql, params, fetch=True)

        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()


def get_hourly_target_times(
    start_time: datetime,
    end_time: datetime
) -> list[datetime]:
    """Generate hourly target times between start and end.

    Args:
        start_time: Start datetime (will be rounded to hour).
        end_time: End datetime (will be rounded to hour).

    Returns:
        List of hourly datetime objects.
    """
    # Round start_time to hour
    current = start_time.replace(minute=0, second=0, microsecond=0)
    if start_time.minute >= 30:
        current += timedelta(hours=1)

    # Round end_time to hour
    end = end_time.replace(minute=0, second=0, microsecond=0)
    if end_time.minute >= 30:
        end += timedelta(hours=1)

    targets = []
    while current <= end:
        targets.append(current)
        current += timedelta(hours=1)

    return targets



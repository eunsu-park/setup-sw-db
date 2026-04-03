"""Export sw_30min table to a single Parquet file.

Exports the entire (or date-filtered) sw_30min table as one Parquet file,
eliminating the need for per-event CSV extraction. The resulting file is
small (~17MB for 10 years) and can be loaded entirely into memory during
model training.
"""
import argparse
from datetime import datetime
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from egghouse.database import PostgresManager
from core import load_config


# Columns to export (matches regression-sw input_variables + target)
SW_30MIN_COLUMNS = [
    'datetime',
    'v_avg', 'v_min', 'v_max',
    'np_avg', 'np_min', 'np_max',
    't_avg', 't_min', 't_max',
    'bx_avg', 'bx_min', 'bx_max',
    'by_avg', 'by_min', 'by_max',
    'bz_avg', 'bz_min', 'bz_max',
    'bt_avg', 'bt_min', 'bt_max',
    'ap30', 'hp30',
]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Export sw_30min table to Parquet file')
    parser.add_argument('-o', '--output', type=str, required=True,
                        help='Output Parquet file path')
    parser.add_argument('-s', '--start', type=str, default=None,
                        help='Start date (YYYY-MM-DD), inclusive')
    parser.add_argument('-e', '--end', type=str, default=None,
                        help='End date (YYYY-MM-DD), inclusive')
    parser.add_argument('--config', type=str,
                        default='configs/space_weather_config.yaml',
                        help='Config file path')
    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config['db_config']

    # Build query with optional date filters
    cols_str = ', '.join(SW_30MIN_COLUMNS)
    conditions = []
    params = []

    if args.start:
        conditions.append('datetime >= %s')
        params.append(datetime.strptime(args.start, '%Y-%m-%d'))
    if args.end:
        conditions.append('datetime <= %s')
        params.append(datetime.strptime(args.end, '%Y-%m-%d').replace(
            hour=23, minute=59, second=59))

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT {cols_str} FROM sw_30min{where_clause} ORDER BY datetime"

    print(f"[Export sw_30min to Parquet]")
    print(f"Query: {sql}")
    if params:
        print(f"Params: {params}")
    print()

    # Execute query
    with PostgresManager(**db_config) as db:
        rows = db.execute(sql, tuple(params) if params else None, fetch=True)

    if not rows:
        print("No data found.")
        return

    df = pd.DataFrame(rows, columns=SW_30MIN_COLUMNS)
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Write Parquet
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine='pyarrow')

    # Summary
    file_size = os.path.getsize(output_path)
    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]

    print(f"Exported: {output_path}")
    print(f"Rows: {len(df):,}")
    print(f"Date range: {df['datetime'].min()} ~ {df['datetime'].max()}")
    print(f"File size: {file_size / 1024 / 1024:.1f} MB")
    if len(nan_cols) > 0:
        print(f"\nNaN counts per column:")
        for col, count in nan_cols.items():
            print(f"  {col}: {count:,}")
    else:
        print(f"NaN: none")


if __name__ == '__main__':
    main()

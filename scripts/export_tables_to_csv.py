"""Export space_weather DB tables to CSV files.

Exports OMNI (1-min / 5-min / 1-hour) and GOES (XRS / MAG / proton)
tables as one CSV per table into a target directory. Uses PostgreSQL
COPY TO STDOUT for efficient server-side streaming.
"""
import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from egghouse.database import PostgresManager
from core import load_config


# (source_table, output_basename, order_by) — output file is <basename>.csv
TABLES = [
    ('omni_high_resolution',      'omni_1min',  'datetime'),
    ('omni_high_resolution_5min', 'omni_5min',  'datetime'),
    ('omni_low_resolution',       'omni_1hour', 'datetime'),
    ('goes_xrs',                  'goes_xrs',   'satellite, datetime'),
    ('goes_mag',                  'goes_mag',   'satellite, datetime'),
    ('goes_proton',               'goes_proton', 'satellite, datetime'),
]

DEFAULT_OUTPUT_DIR = '/opt/nas/ap_share/dataset/'


def export_table(db, table: str, order_by: str, output_path: Path) -> int:
    """Stream a table to CSV via PostgreSQL COPY TO STDOUT.

    Args:
        db: Active PostgresManager instance.
        table: Source table name.
        order_by: SQL ORDER BY clause (columns only, no keyword).
        output_path: Destination CSV file path.

    Returns:
        Number of rows exported.
    """
    copy_sql = (
        f"COPY (SELECT * FROM {table} ORDER BY {order_by}) "
        f"TO STDOUT WITH CSV HEADER"
    )
    with db.conn.cursor() as cur, open(output_path, 'w') as f:
        cur.copy_expert(copy_sql, f)
        return cur.rowcount


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Export space_weather DB tables to CSV files')
    parser.add_argument(
        '-o', '--output-dir', type=str, default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})')
    parser.add_argument(
        '--config', type=str,
        default='configs/space_weather_config.yaml',
        help='Config file path')
    parser.add_argument(
        '--tables', type=str, nargs='+', default=None,
        help='Subset of output names to export (default: all). '
             'Choices: ' + ', '.join(t[1] for t in TABLES))
    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config['db_config']

    selected = TABLES
    if args.tables:
        requested = set(args.tables)
        selected = [t for t in TABLES if t[1] in requested or t[0] in requested]
        if not selected:
            print(f"No matching tables. Available: "
                  f"{[t[1] for t in TABLES]}")
            return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[Export space_weather tables to CSV]")
    print(f"Output dir: {output_dir}")
    print(f"Tables: {[t[1] for t in selected]}")
    print()

    total_rows = 0
    total_bytes = 0
    failed = []

    with PostgresManager(**db_config) as db:
        for src_table, out_name, order_by in selected:
            csv_path = output_dir / f'{out_name}.csv'
            print(f"-> {src_table}  =>  {csv_path}")
            try:
                rows = export_table(db, src_table, order_by, csv_path)
                size = csv_path.stat().st_size
                total_rows += rows
                total_bytes += size
                print(f"   rows={rows:,}  size={size / 1024 / 1024:.1f} MB")
            except Exception as e:
                failed.append((src_table, str(e)))
                print(f"   FAILED: {e}")

    print()
    print(f"Done. rows={total_rows:,}  "
          f"size={total_bytes / 1024 / 1024:.1f} MB")
    if failed:
        print(f"Failed tables:")
        for name, err in failed:
            print(f"  - {name}: {err}")


if __name__ == '__main__':
    main()

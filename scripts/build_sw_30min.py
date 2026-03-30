"""Build and query the sw_30min aggregation table.

Aggregates OMNI 1-min and HPo 30-min data into 30-minute cadence,
then stores the result in the sw_30min table.
"""
import argparse
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from egghouse.database import PostgresManager
from core import load_config, aggregate_sw_30min, extract_event_data, insert


def build_year(year: int, db_config: dict) -> int:
    """Build sw_30min data for a single year.

    Deletes existing records for the year, then inserts new aggregated data.

    Args:
        year: Year to process.
        db_config: Database configuration.

    Returns:
        Number of records inserted.
    """
    start = datetime(year, 1, 1, 0, 0, 0)
    end = datetime(year, 12, 31, 23, 30, 0)

    df = aggregate_sw_30min(start, end, db_config)
    if df.empty:
        return 0

    # Delete existing records for this year, then insert
    with PostgresManager(**db_config) as db:
        db.execute(
            "DELETE FROM sw_30min WHERE datetime >= %s AND datetime <= %s",
            (start, end)
        )

    count = insert(df, 'sw_30min', db_config, batch=5000)
    return count


def build(args, db_config: dict):
    """Build sw_30min table for year range.

    Args:
        args: Parsed CLI arguments.
        db_config: Database configuration.
    """
    print("[Build sw_30min]")
    print(f"Year range: {args.start_year} ~ {args.end_year}")
    print()

    total = 0
    for year in range(args.start_year, args.end_year + 1):
        count = build_year(year, db_config)
        print(f"  {year}: {count} records")
        total += count

    print(f"\nTotal: {total} records")


def extract(args, db_config: dict):
    """Extract event data from sw_30min table.

    Args:
        args: Parsed CLI arguments.
        db_config: Database configuration.
    """
    T = datetime.strptime(args.time, '%Y-%m-%d %H:%M:%S')
    df = extract_event_data(T, B=args.before, A=args.after, db_config=db_config)

    print(f"[Extract sw_30min]")
    print(f"T = {T}, B = {args.before}, A = {args.after}")
    print(f"Records: {len(df)}")

    if df.empty:
        print("No data found.")
        return

    if args.output:
        df.to_csv(args.output, index=False)
        print(f"Saved to: {args.output}")
    else:
        print(df.to_string(index=False))


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Build/Query sw_30min table')
    parser.add_argument('--config', type=str,
                        default='configs/space_weather_config.yaml',
                        help='Config file path')

    subparsers = parser.add_subparsers(dest='command', required=True)

    # Build subcommand
    build_parser = subparsers.add_parser('build', help='Build sw_30min table')
    build_parser.add_argument('--start-year', type=int, default=2010,
                              help='Start year (default: 2010)')
    build_parser.add_argument('--end-year', type=int, default=2025,
                              help='End year (default: 2025)')

    # Extract subcommand
    extract_parser = subparsers.add_parser('extract',
                                           help='Extract event data')
    extract_parser.add_argument('-t', '--time', type=str, required=True,
                                help='Reference time T (YYYY-MM-DD HH:MM:SS)')
    extract_parser.add_argument('-b', '--before', type=int, default=5,
                                help='Days before T (default: 5)')
    extract_parser.add_argument('-a', '--after', type=int, default=3,
                                help='Days after T (default: 3)')
    extract_parser.add_argument('-o', '--output', type=str, default=None,
                                help='Output CSV path (default: print to stdout)')

    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config['db_config']

    if args.command == 'build':
        build(args, db_config)
    elif args.command == 'extract':
        extract(args, db_config)


if __name__ == '__main__':
    main()

"""HPo geomagnetic index downloader with DB integration.

Downloads Hp30/Hp60 data from GFZ Potsdam and stores in space_weather database.
Supports two modes:
  Year-based (default): Download via JSON API per year, replace by year.
  --nowcast:            Download last 30 days from text endpoint, upsert new records.
"""
import argparse
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    load_config, download, download_json, parse_hpo, parse_hpo_json,
    insert, upsert, HP30, HP60,
)


def process_year(year: int, spec: dict, db_config: dict,
                 download_config: dict) -> int:
    """Download and ingest one year of HPo data via JSON API.

    Args:
        year: Year to process.
        spec: Data specification (HP30 or HP60).
        db_config: Database configuration.
        download_config: Download configuration for this table.

    Returns:
        Number of records inserted.
    """
    url_pattern = download_config['url_pattern']
    indexes = download_config['indexes']
    table = spec['table']

    hp_url = url_pattern.format(year=year, index=indexes[0])
    ap_url = url_pattern.format(year=year, index=indexes[1])

    hp_data = download_json(hp_url)
    if hp_data is None:
        return 0

    ap_data = download_json(ap_url)
    if ap_data is None:
        return 0

    df = parse_hpo_json(hp_data, ap_data, spec)
    count = insert(df, table, db_config, replace_key={'year': year})

    return count


def process_nowcast(spec: dict, db_config: dict,
                    download_config: dict) -> int:
    """Download and ingest last 30 days of HPo data via text endpoint.

    Args:
        spec: Data specification (HP30 or HP60).
        db_config: Database configuration.
        download_config: Download configuration for this table.

    Returns:
        Number of records inserted.
    """
    url = download_config['url_nowcast']
    table = spec['table']

    text = download(url, timeout=30)
    if text is None:
        return 0

    df = parse_hpo(text, spec)
    count = upsert(df, table, db_config, conflict_columns='datetime')

    return count


def main():
    """Main entry point."""
    current_year = datetime.now().year

    parser = argparse.ArgumentParser(description='HPo Geomagnetic Index Downloader')
    parser.add_argument('--hp30', action='store_true',
                        help='Download Hp30 (30-min) data')
    parser.add_argument('--hp60', action='store_true',
                        help='Download Hp60 (60-min) data')
    parser.add_argument('--all', action='store_true',
                        help='Download both Hp30 and Hp60')
    parser.add_argument('--start', type=int, default=1985,
                        help='Start year (default: 1985)')
    parser.add_argument('--end', type=int, default=current_year,
                        help=f'End year (default: {current_year})')
    parser.add_argument('--nowcast', action='store_true',
                        help='Download last 30 days (incremental upsert)')
    parser.add_argument('--config', type=str,
                        default='configs/space_weather_config.yaml',
                        help='Config file path')

    args = parser.parse_args()

    if args.all:
        args.hp30 = True
        args.hp60 = True

    if not args.hp30 and not args.hp60:
        parser.error('Specify --hp30, --hp60, or --all')

    config = load_config(args.config)
    db_config = config['db_config']
    download_config = config['download_config']

    print("HPo Geomagnetic Index Downloader")
    print(f"Database: {db_config.get('database', 'unknown')}")
    if args.nowcast:
        print("Mode: nowcast (last 30 days)")
    else:
        print(f"Year range: {args.start} to {args.end}")
    print()

    if args.hp30:
        print("[Hp30 - 30min resolution]")
        hp30_config = download_config.get('hpo_hp30')
        if args.nowcast:
            count = process_nowcast(HP30, db_config, hp30_config)
            print(f"  {count} records inserted")
        else:
            for year in range(args.start, args.end + 1):
                count = process_year(year, HP30, db_config, hp30_config)
                print(f"  {year}: {count} records")

    if args.hp60:
        print("\n[Hp60 - 60min resolution]")
        hp60_config = download_config.get('hpo_hp60')
        if args.nowcast:
            count = process_nowcast(HP60, db_config, hp60_config)
            print(f"  {count} records inserted")
        else:
            for year in range(args.start, args.end + 1):
                count = process_year(year, HP60, db_config, hp60_config)
                print(f"  {year}: {count} records")


if __name__ == '__main__':
    main()

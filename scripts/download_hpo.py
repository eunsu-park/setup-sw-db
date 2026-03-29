"""HPo geomagnetic index downloader with DB integration.

Downloads Hp30/Hp60 data from GFZ Potsdam and stores in space_weather database.
Supports two modes:
  --mode complete: Download full historical series (1985-present), truncate and bulk insert.
  --mode nowcast:  Download last 30 days, upsert new records only.
"""
import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from egghouse.database import PostgresManager

from core import load_config, download, parse_hpo, insert, upsert, HP30, HP60


def process_hpo(spec: dict, db_config: dict, download_config: dict,
                mode: str = 'nowcast') -> int:
    """Download and ingest one HPo dataset.

    Args:
        spec: Data specification (HP30 or HP60).
        db_config: Database configuration.
        download_config: Download configuration for this table.
        mode: 'complete' for full series, 'nowcast' for last 30 days.

    Returns:
        Number of records inserted.
    """
    url_key = f'url_{mode}'
    url = download_config[url_key]
    table = spec['table']

    timeout = 120 if mode == 'complete' else 30
    text = download(url, timeout=timeout)
    if text is None:
        return 0

    df = parse_hpo(text, spec)

    if mode == 'complete':
        with PostgresManager(**db_config) as db:
            db.execute(f"TRUNCATE TABLE {table}")
        count = insert(df, table, db_config)
    else:
        count = upsert(df, table, db_config, conflict_columns='datetime')

    return count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='HPo Geomagnetic Index Downloader')
    parser.add_argument('--hp30', action='store_true',
                        help='Download Hp30 (30-min) data')
    parser.add_argument('--hp60', action='store_true',
                        help='Download Hp60 (60-min) data')
    parser.add_argument('--all', action='store_true',
                        help='Download both Hp30 and Hp60')
    parser.add_argument('--mode', choices=['complete', 'nowcast'],
                        default='nowcast',
                        help='Download mode (default: nowcast)')
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
    print(f"Mode: {args.mode}")
    print()

    if args.hp30:
        print("[Hp30 - 30min resolution]")
        hp30_config = download_config.get('hpo_hp30')
        count = process_hpo(HP30, db_config, hp30_config, mode=args.mode)
        print(f"  {count} records inserted")

    if args.hp60:
        print("\n[Hp60 - 60min resolution]")
        hp60_config = download_config.get('hpo_hp60')
        count = process_hpo(HP60, db_config, hp60_config, mode=args.mode)
        print(f"  {count} records inserted")


if __name__ == '__main__':
    main()

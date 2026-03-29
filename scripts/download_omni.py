"""OMNI data downloader with DB integration.

Downloads OMNI solar wind data and stores in space_weather database.
"""
import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import load_config, download, parse, insert, LOWRES, HIGHRES, HIGHRES_5MIN


def process_year(year: int, spec: dict, db_config: dict, download_config: dict) -> int:
    """Process one year of OMNI data.

    Args:
        year: Year to process.
        spec: Data specification (LOWRES or HIGHRES).
        db_config: Database configuration.
        download_config: Download configuration.

    Returns:
        Number of records inserted.
    """
    url = download_config['url_pattern'].format(year=year)

    text = download(url)
    if text is None:
        return 0

    df = parse(text, spec)
    count = insert(df, spec['table'], db_config, replace_key={'year': year})

    return count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='OMNI Data Downloader')
    parser.add_argument('--lowres', action='store_true',
                        help='Download low resolution (hourly) data')
    parser.add_argument('--highres', action='store_true',
                        help='Download high resolution (1-min) data')
    parser.add_argument('--highres-5min', action='store_true',
                        help='Download high resolution (5-min) data')
    parser.add_argument('--all', action='store_true',
                        help='Download all resolutions (lowres + highres + highres-5min)')
    parser.add_argument('--start', type=int, default=2020,
                        help='Start year (default: 2020)')
    parser.add_argument('--end', type=int, default=2024,
                        help='End year (default: 2024)')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/space_weather_config.yaml',
                        help='Config file path (default: configs/space_weather_config.yaml)')

    args = parser.parse_args()

    if args.all:
        args.lowres = True
        args.highres = True
        args.highres_5min = True

    config = load_config(args.config)
    db_config = config['db_config']
    download_config = config['download_config']

    print("OMNI Data Downloader")
    print(f"Database: {db_config.get('database', 'unknown')}")
    print(f"Year range: {args.start} to {args.end}")
    print()

    if args.lowres:
        print("[Low Resolution]")
        low_config = download_config.get('omni_low_resolution')
        for year in range(args.start, args.end + 1):
            count = process_year(year, LOWRES, db_config, low_config)
            print(f"  {year}: {count} records")

    if args.highres:
        print("\n[High Resolution]")
        high_config = download_config.get('omni_high_resolution')
        for year in range(args.start, args.end + 1):
            count = process_year(year, HIGHRES, db_config, high_config)
            print(f"  {year}: {count} records")

    if args.highres_5min:
        print("\n[High Resolution 5-min]")
        high5_config = download_config.get('omni_high_resolution_5min')
        for year in range(args.start, args.end + 1):
            count = process_year(year, HIGHRES_5MIN, db_config, high5_config)
            print(f"  {year}: {count} records")


if __name__ == '__main__':
    main()

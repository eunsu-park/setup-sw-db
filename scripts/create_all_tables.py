"""Create all database tables.

Creates tables for:
- space_weather: OMNI time series data
- solar_images: LASCO, SDO, SECCHI image metadata
"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import load_config, create_database, create_tables


CONFIGS = [
    'configs/space_weather_config.yaml',
    'configs/solar_images_config.yaml',
]


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Create all database tables')
    parser.add_argument('--drop', action='store_true',
                        help='Drop existing tables before creating')
    args = parser.parse_args()

    for config_path in CONFIGS:
        config_file = Path(config_path)
        if not config_file.exists():
            print(f"\n[SKIP] Config not found: {config_path}")
            continue

        print(f"\n{'='*60}")
        print(f"Config: {config_path}")
        print('='*60)

        config = load_config(config_path)
        db_config = config['db_config']
        schema_config = config['schema_config']

        create_database(db_config)
        create_tables(db_config, schema_config, drop=args.drop)


if __name__ == '__main__':
    main()

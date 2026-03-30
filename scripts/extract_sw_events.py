"""Batch extract event data from sw_30min table.

Iterates over a time range at a given cadence, extracting event windows
and saving each as an individual CSV file. Skips events with NaN values.
"""
import argparse
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from core import load_config, extract_event_data


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Batch extract sw_30min event data to CSV files')
    parser.add_argument('-s', '--start', type=str, required=True,
                        help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('-e', '--end', type=str, required=True,
                        help='End time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('-c', '--cadence', type=int, default=30,
                        help='T iteration cadence in minutes (default: 30)')
    parser.add_argument('-b', '--before', type=int, default=5,
                        help='Days before T (default: 5)')
    parser.add_argument('-a', '--after', type=int, default=3,
                        help='Days after T (default: 3)')
    parser.add_argument('-o', '--output-dir', type=str, required=True,
                        help='Output directory for CSV files')
    parser.add_argument('--config', type=str,
                        default='configs/space_weather_config.yaml',
                        help='Config file path')
    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config['db_config']

    start = datetime.strptime(args.start, '%Y-%m-%d %H:%M:%S')
    end = datetime.strptime(args.end, '%Y-%m-%d %H:%M:%S')
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range(start, end, freq=f'{args.cadence}min')

    print(f"[Extract sw_30min events]")
    print(f"Range: {start} ~ {end}")
    print(f"Cadence: {args.cadence} min, B={args.before}, A={args.after}")
    print(f"Total T points: {len(timestamps)}")
    print(f"Output: {output_dir}")
    print()

    saved = 0
    skipped_nan = 0
    skipped_empty = 0

    for i, T in enumerate(timestamps):
        T_dt = T.to_pydatetime()
        df = extract_event_data(T_dt, B=args.before, A=args.after,
                                db_config=db_config)

        if df.empty:
            skipped_empty += 1
            continue

        if df.isna().any().any():
            skipped_nan += 1
            continue

        filename = T.strftime('%Y%m%d%H%M%S') + '.csv'
        df.to_csv(output_dir / filename, index=False)
        saved += 1

        if (i + 1) % 100 == 0:
            print(f"  [{i + 1}/{len(timestamps)}] "
                  f"saved={saved}, skipped(NaN)={skipped_nan}, "
                  f"skipped(empty)={skipped_empty}")

    print(f"\nDone: saved={saved}, skipped(NaN)={skipped_nan}, "
          f"skipped(empty)={skipped_empty}")


if __name__ == '__main__':
    main()

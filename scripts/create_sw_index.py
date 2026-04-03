"""Create train/validation/test index files from sw_30min Parquet.

Generates lightweight CSV index files containing valid reference datetimes
and labels. NaN validation is done once with the maximum possible window,
guaranteeing any smaller experiment window is also NaN-free.
"""
import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Create train/val/test index files from sw_30min Parquet')
    parser.add_argument('-d', '--data', type=str, required=True,
                        help='Path to sw_30min.parquet')
    parser.add_argument('-o', '--output-dir', type=str, required=True,
                        help='Output directory for index CSV files')
    parser.add_argument('-c', '--cadence', type=int, default=30,
                        help='Reference time cadence in minutes (default: 30)')
    parser.add_argument('--max-before', type=int, default=240,
                        help='Max input timesteps before T (default: 240 = 5 days)')
    parser.add_argument('--max-after', type=int, default=144,
                        help='Max target timesteps after T (default: 144 = 3 days)')
    parser.add_argument('--label-threshold', type=int, default=30,
                        help='ap30 threshold for storm label (default: 30, NOAA G1)')
    parser.add_argument('--label-column', type=str, default='ap30',
                        help='Column to compute labels from (default: ap30)')
    parser.add_argument('--train-start', type=str, required=True,
                        help='Train split start date (YYYY-MM-DD)')
    parser.add_argument('--train-end', type=str, required=True,
                        help='Train split end date (YYYY-MM-DD)')
    parser.add_argument('--val-start', type=str, required=True,
                        help='Validation split start date (YYYY-MM-DD)')
    parser.add_argument('--val-end', type=str, required=True,
                        help='Validation split end date (YYYY-MM-DD)')
    parser.add_argument('--test-start', type=str, default=None,
                        help='Test split start date (optional)')
    parser.add_argument('--test-end', type=str, default=None,
                        help='Test split end date (optional)')
    parser.add_argument('--prefix', type=str, default=None,
                        help='Output filename prefix (default: generates '
                             'train_index.csv, etc.)')
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load Parquet
    print(f"[Create sw_30min index]")
    print(f"Loading: {args.data}")
    df = pd.read_parquet(args.data)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').reset_index(drop=True)
    print(f"Rows: {len(df):,}, "
          f"Range: {df['datetime'].min()} ~ {df['datetime'].max()}")

    # Numeric columns (exclude datetime)
    numeric_cols = [c for c in df.columns if c != 'datetime']
    data_array = df[numeric_cols].values.astype(np.float32)

    # Precompute per-row NaN mask (True = has NaN)
    row_has_nan = np.any(np.isnan(data_array), axis=1)

    # Build datetime → row index mapping
    datetimes = df['datetime'].values  # numpy datetime64
    dt_to_row = {dt: i for i, dt in enumerate(datetimes)}

    # Find label column index
    label_col_idx = numeric_cols.index(args.label_column)

    # Generate candidate reference times from data range
    data_start = df['datetime'].min()
    data_end = df['datetime'].max()
    candidates = pd.date_range(
        start=data_start + pd.Timedelta(minutes=args.cadence * args.max_before),
        end=data_end - pd.Timedelta(minutes=args.cadence * args.max_after),
        freq=f'{args.cadence}min'
    )
    print(f"Candidates: {len(candidates):,} (cadence={args.cadence}min, "
          f"max_before={args.max_before}, max_after={args.max_after})")

    # Validate each candidate: check full window is NaN-free
    valid_times = []
    valid_labels = []
    skipped_nan = 0
    skipped_missing = 0

    for T in candidates:
        T_np = np.datetime64(T)
        if T_np not in dt_to_row:
            skipped_missing += 1
            continue

        ref_row = dt_to_row[T_np]
        window_start = ref_row - args.max_before
        window_end = ref_row + args.max_after

        # Bounds check
        if window_start < 0 or window_end > len(data_array):
            skipped_missing += 1
            continue

        # NaN check over full window
        if np.any(row_has_nan[window_start:window_end]):
            skipped_nan += 1
            continue

        # Compute label from target window (after T)
        target_vals = data_array[ref_row:window_end, label_col_idx]
        label = 1 if np.max(target_vals) >= args.label_threshold else 0

        valid_times.append(T)
        valid_labels.append(label)

    print(f"Valid: {len(valid_times):,}, "
          f"Skipped(NaN): {skipped_nan:,}, "
          f"Skipped(missing): {skipped_missing:,}")

    # Split by date ranges
    valid_df = pd.DataFrame({
        'datetime': valid_times,
        'label': valid_labels,
    })

    splits = {
        'train': (args.train_start, args.train_end),
        'validation': (args.val_start, args.val_end),
    }
    if args.test_start and args.test_end:
        splits['test'] = (args.test_start, args.test_end)

    # Output filename pattern
    prefix = args.prefix
    if prefix:
        name_fn = lambda phase: f"{prefix}_{phase}.csv"
    else:
        name_fn = lambda phase: f"{phase}_index.csv"

    print(f"\n--- Splits ---")
    for phase, (start, end) in splits.items():
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        mask = (valid_df['datetime'] >= start_dt) & (valid_df['datetime'] <= end_dt)
        split_df = valid_df[mask].copy()

        out_path = output_dir / name_fn(phase)
        split_df.to_csv(out_path, index=False)

        n_storm = split_df['label'].sum()
        n_quiet = len(split_df) - n_storm
        print(f"{phase}: {len(split_df):,} samples "
              f"(storm={n_storm:,}, quiet={n_quiet:,}) "
              f"→ {out_path}")

    print("\nDone.")


if __name__ == '__main__':
    main()

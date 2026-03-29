"""JSOC Query executor for SDO data - saves URL list to JSON file.

Queries JSOC for SDO data files and saves download URLs to a JSON file
for later use with download_from_urls.py.
"""
import argparse
import datetime
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import load_config, query_jsoc_time_range, check_db_exists_in_range

# Telescope + channel structure
TELESCOPES = {
    'aia': ['193', '211', '171', '304', '94', '131', '335'],
    'hmi': ['m_45s', 'm_720s'],
}


def is_spike_file(filename: str) -> bool:
    """Check if filename contains 'spike' (AIA spike files to exclude).

    Args:
        filename: Filename to check.

    Returns:
        True if file is a spike file, False otherwise.
    """
    return 'spike' in filename.lower()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='JSOC Query for SDO - saves URL list to JSON file'
    )

    # Telescope/channel options
    parser.add_argument('--telescope', type=str, choices=['aia', 'hmi'],
                        default='aia',
                        help='Telescope to query (default: aia)')
    parser.add_argument('--channels', type=str, nargs='+',
                        help='Channels to query (default: all for telescope)')

    # Target time mode (required)
    parser.add_argument('--target-time', type=str, required=True,
                        help='Target time (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)')
    parser.add_argument('--time-range', type=int, default=6,
                        help='Search range in minutes (default: 6)')

    # Output options
    parser.add_argument('--output', '-o', type=str, required=True,
                        help='Output JSON file path')

    # JSOC options
    parser.add_argument('--email', type=str, default=None,
                        help='JSOC registered email (overrides config)')
    parser.add_argument('--include-spike', action='store_true',
                        help='Include spike files for AIA (default: exclude)')
    parser.add_argument('--skip-db-check', action='store_true',
                        help='Skip DB check and always query JSOC')

    # Config options
    parser.add_argument('--config', type=str,
                        default='configs/solar_images_config.yaml',
                        help='Config file path (default: configs/solar_images_config.yaml)')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    db_config = config['db_config']
    jsoc_config = config['jsoc_config']

    # Override email from CLI if provided
    if args.email:
        jsoc_config['email'] = args.email

    telescope = args.telescope
    channels = args.channels or TELESCOPES.get(telescope, [])

    # Parse target time
    try:
        if ' ' in args.target_time:
            target_time = datetime.datetime.strptime(args.target_time, '%Y-%m-%d %H:%M:%S')
        else:
            target_time = datetime.datetime.strptime(args.target_time, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid target time format: {args.target_time}")
        print("Use YYYY-MM-DD HH:MM:SS or YYYY-MM-DD")
        sys.exit(1)

    print("SDO JSOC Query")
    print(f"Target time: {target_time}")
    print(f"Time range: +/- {args.time_range} minutes")
    print(f"Telescope: {telescope}")
    print(f"Channels: {channels}")
    print(f"Output file: {args.output}")
    print()

    # Build output structure
    output_data = {
        'query_time': datetime.datetime.now().isoformat(),
        'target_time': target_time.isoformat(),
        'time_range': args.time_range,
        'telescope': telescope,
        'channels': {},
    }

    total_files = 0
    skipped_count = 0

    for channel in channels:
        key = f"{telescope}_{channel}"
        print(f"[{telescope.upper()} / {channel}]")

        # Check if record already exists in DB within time range
        if not args.skip_db_check and check_db_exists_in_range(
            target_time, args.time_range, 'sdo', db_config,
            telescope=telescope, channel=channel
        ):
            print(f"  Already exists in DB for {target_time} ± {args.time_range}min, skipping")
            skipped_count += 1
            print()
            continue

        # Query JSOC
        print(f"  Querying JSOC for {target_time} +/- {args.time_range} min...")
        files = query_jsoc_time_range(key, target_time, args.time_range, jsoc_config)

        if not files:
            print("  No files found")
            output_data['channels'][channel] = {'files': []}
            print()
            continue

        # Filter spike files for AIA unless --include-spike
        if telescope == 'aia' and not args.include_spike:
            original_count = len(files)
            files = [f for f in files if not is_spike_file(f['filename'])]
            spike_count = original_count - len(files)
            if spike_count > 0:
                print(f"  Skipped {spike_count} spike files")

        print(f"  Found {len(files)} files")

        # Save to output structure
        output_data['channels'][channel] = {
            'files': [
                {'url': f['url'], 'filename': f['filename']}
                for f in files
            ],
        }

        total_files += len(files)
        print()

    print("=" * 50)

    # Skip JSON creation if all channels were skipped (already in DB)
    if skipped_count == len(channels):
        print("All channels already in DB, no JSON created")
        return

    # Skip JSON creation if no files to download
    if total_files == 0:
        print("No files to download, no JSON created")
        return

    # Write output JSON
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"Total files queried: {total_files}")
    print(f"Output saved to: {args.output}")


if __name__ == '__main__':
    main()

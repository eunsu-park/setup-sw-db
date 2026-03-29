"""Shared CLI utilities for download scripts."""
import datetime
from argparse import ArgumentParser


def add_date_arguments(parser: ArgumentParser, default_days: int = 7) -> None:
    """Add common date arguments to argument parser.

    Adds --start-date, --end-date, and --days arguments.

    Args:
        parser: ArgumentParser instance.
        default_days: Default number of days when no date is specified.
    """
    parser.add_argument('--start-date', type=str,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=default_days,
                        help=f'Number of recent days to download (default: {default_days})')


def add_download_arguments(parser: ArgumentParser,
                           default_parallel: int = 4) -> None:
    """Add common download arguments to argument parser.

    Adds --overwrite and --parallel arguments.

    Args:
        parser: ArgumentParser instance.
        default_parallel: Default number of parallel downloads.
    """
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files')
    parser.add_argument('--parallel', type=int, default=default_parallel,
                        help=f'Number of parallel downloads (default: {default_parallel})')


def add_db_arguments(parser: ArgumentParser) -> None:
    """Add common database arguments to argument parser.

    Adds --init-db argument.

    Args:
        parser: ArgumentParser instance.
    """
    parser.add_argument('--init-db', action='store_true',
                        help='Initialize database and tables')


def parse_date_range(args, mission_start: datetime.date = None
                     ) -> tuple[datetime.date, datetime.date]:
    """Parse date range from command line arguments.

    Args:
        args: Parsed arguments with start_date, end_date, and days attributes.
        mission_start: Optional earliest valid date. Dates before this are adjusted.

    Returns:
        Tuple of (start_date, end_date).
    """
    if args.start_date:
        start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=args.days - 1)

    if args.end_date:
        end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = datetime.date.today()

    # Validate against mission start date
    if mission_start and start_date < mission_start:
        print(f"Warning: Adjusting start date from {start_date} to {mission_start}")
        start_date = mission_start

    return start_date, end_date

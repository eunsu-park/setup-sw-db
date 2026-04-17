"""Unified CLI entry point for setup-sw-db.

Dispatches subcommands to existing scripts via subprocess. Provides a
single `swdb` interface for all database and download operations.

Usage:
    swdb download omni --all --start 2020 --end 2024
    swdb download hpo --all --start 2020 --end 2024
    swdb download sdo --telescope aia --start-date 2024-01-01 --end-date 2024-01-31
    swdb register sdo [scan_dir]
    swdb build sw-30min --start-year 2020 --end-year 2024
    swdb extract events -o ./events/
    swdb db init [--drop]
    swdb db status
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Project root: parent of core/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _run_script(script_name: str, extra_args: list[str]) -> int:
    """Run a script from the scripts/ directory with extra arguments.

    Args:
        script_name: Script filename (e.g., 'download_omni.py').
        extra_args: Additional CLI arguments to pass to the script.

    Returns:
        Process return code.
    """
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"Error: script not found: {script_path}")
        return 1

    cmd = [sys.executable, str(script_path)] + extra_args
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return result.returncode


def cmd_download_omni(args: argparse.Namespace) -> int:
    """Handle 'swdb download omni' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = []
    if args.all:
        extra.append("--all")
    else:
        if args.lowres:
            extra.append("--lowres")
        if args.highres:
            extra.append("--highres")
        if args.highres_5min:
            extra.append("--highres-5min")
    extra += ["--start", str(args.start), "--end", str(args.end)]
    return _run_script("download_omni.py", extra)


def cmd_download_hpo(args: argparse.Namespace) -> int:
    """Handle 'swdb download hpo' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = []
    if args.all:
        extra.append("--all")
    else:
        if args.hp30:
            extra.append("--hp30")
        if args.hp60:
            extra.append("--hp60")
    extra += ["--start", str(args.start), "--end", str(args.end)]
    if args.nowcast:
        extra.append("--nowcast")
    return _run_script("download_hpo.py", extra)


def cmd_download_sdo(args: argparse.Namespace) -> int:
    """Handle 'swdb download sdo' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = ["--telescope", args.telescope]
    if args.channels:
        extra += ["--channels"] + args.channels
    if args.start_date:
        extra += ["--start-date", args.start_date]
    if args.end_date:
        extra += ["--end-date", args.end_date]
    if args.init_db:
        extra.append("--init-db")
    if args.overwrite:
        extra.append("--overwrite")
    if args.parallel:
        extra += ["--parallel", str(args.parallel)]
    return _run_script("download_sdo.py", extra)


def cmd_download_goes(args: argparse.Namespace) -> int:
    """Handle 'swdb download goes' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = [
        "--instrument", args.instrument,
        "--satellites", *[str(s) for s in args.satellites],
        "--start-date", args.start_date,
        "--end-date", args.end_date,
    ]
    if args.parallel is not None:
        extra += ["--parallel", str(args.parallel)]
    if args.overwrite:
        extra.append("--overwrite")
    if args.init_db:
        extra.append("--init-db")
    return _run_script("download_goes.py", extra)


def cmd_register_goes(args: argparse.Namespace) -> int:
    """Handle 'swdb register goes' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = [
        "--instrument", args.instrument,
        "--satellites", *[str(s) for s in args.satellites],
    ]
    if args.init_db:
        extra.append("--init-db")
    if args.verbose:
        extra.append("--verbose")
    return _run_script("register_goes.py", extra)


def cmd_register_sdo(args: argparse.Namespace) -> int:
    """Handle 'swdb register sdo' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = []
    if args.scan_dir:
        extra.append(args.scan_dir)
    if args.init_db:
        extra.append("--init-db")
    if args.no_move:
        extra.append("--no-move")
    if args.parallel:
        extra += ["--parallel", str(args.parallel)]
    if args.verbose:
        extra.append("--verbose")
    return _run_script("register_sdo.py", extra)


def cmd_build_sw30min(args: argparse.Namespace) -> int:
    """Handle 'swdb build sw-30min' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = [
        "build",
        "--start-year", str(args.start_year),
        "--end-year", str(args.end_year),
    ]
    return _run_script("build_sw_30min.py", extra)


def cmd_extract_events(args: argparse.Namespace) -> int:
    """Handle 'swdb extract events' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = [
        "-s", args.start,
        "-e", args.end,
        "-o", args.output_dir,
    ]
    if args.cadence:
        extra += ["-c", str(args.cadence)]
    if args.before:
        extra += ["-b", str(args.before)]
    if args.after:
        extra += ["-a", str(args.after)]
    return _run_script("extract_sw_events.py", extra)


def cmd_db_init(args: argparse.Namespace) -> int:
    """Handle 'swdb db init' subcommand.

    Args:
        args: Parsed arguments.

    Returns:
        Process return code.
    """
    extra = []
    if args.drop:
        extra.append("--drop")
    return _run_script("create_all_tables.py", extra)


def cmd_db_status(args: argparse.Namespace) -> int:
    """Handle 'swdb db status' subcommand.

    Shows database connectivity, row counts, and date range coverage
    for both space_weather and solar_images databases.

    Args:
        args: Parsed arguments.

    Returns:
        0 on success, 1 on failure.
    """
    from .utils import load_config

    configs = [
        ("space_weather", "configs/space_weather_config.yaml"),
        ("solar_images", "configs/solar_images_config.yaml"),
    ]

    exit_code = 0

    for db_label, config_path in configs:
        full_path = PROJECT_ROOT / config_path
        if not full_path.exists():
            print(f"\n[{db_label}] Config not found: {config_path}")
            continue

        config = load_config(str(full_path))
        db_config = config["db_config"]
        schema_config = config["schema_config"]

        print(f"\n{'=' * 60}")
        print(f"Database: {db_label}")
        print(f"  Host: {db_config.get('host', 'unknown')}")
        print(f"  Port: {db_config.get('port', 5432)}")
        print(f"{'=' * 60}")

        try:
            from egghouse.database import PostgresManager

            with PostgresManager(**db_config) as db:
                print(f"  Connection: OK")

                tables = [t["name"] for t in db.list_tables()]
                schema_tables = list(schema_config.keys())

                for table_name in schema_tables:
                    if table_name not in tables:
                        print(f"\n  [{table_name}] NOT CREATED")
                        continue

                    # Row count
                    result = db.execute(
                        f"SELECT COUNT(*) as cnt FROM {table_name}",
                        fetch=True,
                    )
                    count = result[0]["cnt"] if result else 0

                    # Date range (look for datetime column)
                    has_datetime = False
                    for col_name in schema_config[table_name]:
                        if col_name == "datetime":
                            has_datetime = True
                            break

                    print(f"\n  [{table_name}]")
                    print(f"    Rows: {count:,}")

                    if has_datetime and count > 0:
                        range_result = db.execute(
                            f"SELECT MIN(datetime) as min_dt, "
                            f"MAX(datetime) as max_dt FROM {table_name}",
                            fetch=True,
                        )
                        if range_result:
                            min_dt = range_result[0]["min_dt"]
                            max_dt = range_result[0]["max_dt"]
                            print(f"    Range: {min_dt} ~ {max_dt}")

        except Exception as e:
            print(f"  Connection: FAILED ({e})")
            exit_code = 1

    return exit_code


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser with all subcommands.

    Returns:
        Configured ArgumentParser.
    """
    parser = argparse.ArgumentParser(
        prog="swdb",
        description="Unified CLI for setup-sw-db: solar wind database management",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── download ─────────────────────────────────────────────────────────
    download_parser = subparsers.add_parser("download", help="Download data")
    download_sub = download_parser.add_subparsers(dest="source", required=True)

    # download omni
    omni_parser = download_sub.add_parser("omni", help="Download OMNI data")
    omni_parser.add_argument("--lowres", action="store_true",
                             help="Download low resolution (hourly) data")
    omni_parser.add_argument("--highres", action="store_true",
                             help="Download high resolution (1-min) data")
    omni_parser.add_argument("--highres-5min", action="store_true",
                             help="Download high resolution (5-min) data")
    omni_parser.add_argument("--all", action="store_true",
                             help="Download all resolutions")
    omni_parser.add_argument("--start", type=int, default=2020,
                             help="Start year (default: 2020)")
    omni_parser.add_argument("--end", type=int, default=2024,
                             help="End year (default: 2024)")
    omni_parser.set_defaults(func=cmd_download_omni)

    # download hpo
    hpo_parser = download_sub.add_parser("hpo", help="Download HPo data")
    hpo_parser.add_argument("--hp30", action="store_true",
                            help="Download Hp30 (30-min) data")
    hpo_parser.add_argument("--hp60", action="store_true",
                            help="Download Hp60 (60-min) data")
    hpo_parser.add_argument("--all", action="store_true",
                            help="Download both Hp30 and Hp60")
    hpo_parser.add_argument("--start", type=int, default=1985,
                            help="Start year (default: 1985)")
    hpo_parser.add_argument("--end", type=int, default=2024,
                            help="End year (default: 2024)")
    hpo_parser.add_argument("--nowcast", action="store_true",
                            help="Download last 30 days (incremental upsert)")
    hpo_parser.set_defaults(func=cmd_download_hpo)

    # download sdo
    sdo_dl_parser = download_sub.add_parser("sdo", help="Download SDO data")
    sdo_dl_parser.add_argument("--telescope", type=str, choices=["aia", "hmi"],
                               default="aia", help="Telescope (default: aia)")
    sdo_dl_parser.add_argument("--channels", type=str, nargs="+",
                               help="Channels to download")
    sdo_dl_parser.add_argument("--start-date", type=str,
                               help="Start date (YYYY-MM-DD)")
    sdo_dl_parser.add_argument("--end-date", type=str,
                               help="End date (YYYY-MM-DD)")
    sdo_dl_parser.add_argument("--init-db", action="store_true",
                               help="Initialize database")
    sdo_dl_parser.add_argument("--overwrite", action="store_true",
                               help="Overwrite existing files")
    sdo_dl_parser.add_argument("--parallel", type=int, default=None,
                               help="Parallel downloads")
    sdo_dl_parser.set_defaults(func=cmd_download_sdo)

    # download goes
    goes_dl_parser = download_sub.add_parser("goes",
                                             help="Download GOES netCDF data")
    goes_dl_parser.add_argument("--instrument", type=str, required=True,
                                choices=["xrs", "mag", "proton", "all"],
                                help="Instrument to download")
    goes_dl_parser.add_argument("--satellites", type=int, nargs="+",
                                required=True,
                                help="Satellite numbers (e.g. 16 17 18)")
    goes_dl_parser.add_argument("--start-date", type=str, required=True,
                                help="Start date (YYYY-MM-DD)")
    goes_dl_parser.add_argument("--end-date", type=str, required=True,
                                help="End date (YYYY-MM-DD)")
    goes_dl_parser.add_argument("--parallel", type=int, default=None,
                                help="Concurrent downloads (default: 4)")
    goes_dl_parser.add_argument("--overwrite", action="store_true",
                                help="Overwrite existing files")
    goes_dl_parser.add_argument("--init-db", action="store_true",
                                help="Initialize DB tables")
    goes_dl_parser.set_defaults(func=cmd_download_goes)

    # ── register ─────────────────────────────────────────────────────────
    register_parser = subparsers.add_parser("register",
                                            help="Register files to database")
    register_sub = register_parser.add_subparsers(dest="target", required=True)

    # register sdo
    sdo_reg_parser = register_sub.add_parser("sdo",
                                             help="Register SDO FITS files")
    sdo_reg_parser.add_argument("scan_dir", type=str, nargs="?", default=None,
                                help="Directory to scan for FITS files")
    sdo_reg_parser.add_argument("--init-db", action="store_true",
                                help="Initialize database")
    sdo_reg_parser.add_argument("--no-move", action="store_true",
                                help="Register only, do not move files")
    sdo_reg_parser.add_argument("--parallel", type=int, default=None,
                                help="Parallel validation workers")
    sdo_reg_parser.add_argument("--verbose", "-v", action="store_true",
                                help="Verbose output")
    sdo_reg_parser.set_defaults(func=cmd_register_sdo)

    # register goes
    goes_reg_parser = register_sub.add_parser("goes",
                                              help="Register GOES netCDF files")
    goes_reg_parser.add_argument("--instrument", type=str, required=True,
                                 choices=["xrs", "mag", "proton", "all"],
                                 help="Instrument to register")
    goes_reg_parser.add_argument("--satellites", type=int, nargs="+",
                                 required=True,
                                 help="Satellite numbers (e.g. 16 17 18)")
    goes_reg_parser.add_argument("--init-db", action="store_true",
                                 help="Initialize DB tables")
    goes_reg_parser.add_argument("--verbose", "-v", action="store_true",
                                 help="Verbose output")
    goes_reg_parser.set_defaults(func=cmd_register_goes)

    # ── build ────────────────────────────────────────────────────────────
    build_parser_cmd = subparsers.add_parser("build",
                                             help="Build aggregated tables")
    build_sub = build_parser_cmd.add_subparsers(dest="target", required=True)

    # build sw-30min
    sw30_parser = build_sub.add_parser("sw-30min",
                                       help="Build sw_30min aggregated table")
    sw30_parser.add_argument("--start-year", type=int, default=2010,
                             help="Start year (default: 2010)")
    sw30_parser.add_argument("--end-year", type=int, default=2025,
                             help="End year (default: 2025)")
    sw30_parser.set_defaults(func=cmd_build_sw30min)

    # ── extract ──────────────────────────────────────────────────────────
    extract_parser = subparsers.add_parser("extract",
                                           help="Extract data from database")
    extract_sub = extract_parser.add_subparsers(dest="target", required=True)

    # extract events
    events_parser = extract_sub.add_parser("events",
                                           help="Extract event CSVs")
    events_parser.add_argument("-s", "--start", type=str, required=True,
                               help="Start time (YYYY-MM-DD HH:MM:SS)")
    events_parser.add_argument("-e", "--end", type=str, required=True,
                               help="End time (YYYY-MM-DD HH:MM:SS)")
    events_parser.add_argument("-o", "--output-dir", type=str, required=True,
                               help="Output directory")
    events_parser.add_argument("-c", "--cadence", type=int, default=None,
                               help="Cadence in minutes (default: 30)")
    events_parser.add_argument("-b", "--before", type=int, default=None,
                               help="Days before T (default: 5)")
    events_parser.add_argument("-a", "--after", type=int, default=None,
                               help="Days after T (default: 3)")
    events_parser.set_defaults(func=cmd_extract_events)

    # ── db ───────────────────────────────────────────────────────────────
    db_parser = subparsers.add_parser("db", help="Database management")
    db_sub = db_parser.add_subparsers(dest="action", required=True)

    # db init
    init_parser = db_sub.add_parser("init",
                                    help="Initialize databases and tables")
    init_parser.add_argument("--drop", action="store_true",
                             help="Drop existing tables before creating")
    init_parser.set_defaults(func=cmd_db_init)

    # db status
    status_parser = db_sub.add_parser("status",
                                      help="Show database status")
    status_parser.set_defaults(func=cmd_db_status)

    return parser


def main():
    """Main entry point for the swdb CLI."""
    parser = build_parser()
    args = parser.parse_args()

    if hasattr(args, "func"):
        sys.exit(args.func(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

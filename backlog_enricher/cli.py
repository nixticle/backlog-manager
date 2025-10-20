from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import ConfigError, ensure_directories, load_config
from .db import connect_database, init_database
from .export import export_data
from .hltb_client import enrich_games
from .ingest_backloggd import ingest_backlog
from .invariants import run_validations
from .logging_setup import configure_logging
from .match import match_games
from .review_tui import run_review
from .stats import collect_stats, print_stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="backlog-enricher", description="Enrich Backloggd backlog with HLTB data.")
    parser.add_argument("--config", type=Path, default=Path("config.toml"), help="Path to config.toml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("initdb", help="Initialize the SQLite schema.")

    ingest_parser = subparsers.add_parser("ingest", help="Ingest Backloggd backlog into the database.")
    ingest_parser.add_argument("--dry-run", action="store_true", help="Parse without writing to the database.")

    enrich_parser = subparsers.add_parser("enrich", help="Fetch HowLongToBeat search results.")
    enrich_parser.add_argument("--dry-run", action="store_true", help="Simulate fetch without persisting results.")

    match_parser = subparsers.add_parser("match", help="Match Backloggd titles with HLTB entries.")
    match_parser.add_argument("--dry-run", action="store_true", help="Run matcher without writing matches.")

    review_parser = subparsers.add_parser("review", help="Review queued matches in a TUI.")
    review_parser.add_argument("--dry-run", action="store_true", help="Run review without committing decisions.")

    export_parser = subparsers.add_parser("export", help="Export enriched backlog data.")
    export_parser.add_argument("formats", nargs="*", help="Formats to export (csv, json, parquet).")

    subparsers.add_parser("stats", help="Show ETL stats.")
    subparsers.add_parser("validate", help="Run invariant checks.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        parser.error(str(exc))
        return 2

    configure_logging(cfg)
    ensure_directories(cfg)

    command = args.command
    if command == "initdb":
        init_database(cfg)
        print(f"Initialized database at {cfg.db_path()}")
        return 0

    with connect_database(cfg) as db:
        if command == "ingest":
            stats = ingest_backlog(cfg, db, dry_run=args.dry_run)
            print(f"Ingest complete: {stats}")
            return 0

        if command == "enrich":
            stats = enrich_games(cfg, db, dry_run=args.dry_run)
            print(f"Enrich complete: {stats}")
            return 0

        if command == "match":
            stats = match_games(cfg, db, dry_run=args.dry_run)
            print(f"Match complete: {stats}")
            return 0

        if command == "review":
            run_review(cfg, db, dry_run=args.dry_run)
            return 0

        if command == "export":
            formats = args.formats or cfg.export.formats
            paths = export_data(cfg, db, formats)
            for fmt, path in paths.items():
                print(f"{fmt}: {path}")
            return 0

        if command == "stats":
            stats = collect_stats(db)
            print_stats(stats)
            return 0

        if command == "validate":
            errors = run_validations(cfg, db)
            if errors:
                for error in errors:
                    print(f"ERROR: {error}", file=sys.stderr)
                return 1
            print("All invariants satisfied.")
            return 0

    parser.error(f"Unknown command {command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())


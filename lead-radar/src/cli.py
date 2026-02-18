"""Command-line interface for Lead Radar."""

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lead Radar CLI")
    parser.add_argument("--version", action="store_true", help="Show version and exit")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.version:
        print("lead-radar 0.1.0")
    else:
        parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

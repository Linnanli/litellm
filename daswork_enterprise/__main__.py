from argparse import ArgumentParser, Namespace
from asyncio import run
from sys import stderr, stdout
from typing import Final

from psycopg import Error

from .schema import run_schema
from .security import smoke_hook


class Commands(Namespace):
    initialize_schema: bool
    smoke_hook: bool


def main() -> int:
    parser: Final = ArgumentParser(prog="python -m daswork_enterprise")
    commands: Final = parser.add_mutually_exclusive_group(required=True)
    commands.add_argument("--initialize-schema", action="store_true")
    commands.add_argument("--check-schema", action="store_true")
    commands.add_argument("--smoke-hook", action="store_true")
    args: Final = parser.parse_args(namespace=Commands())
    try:
        if args.smoke_hook:
            run(smoke_hook())
            stdout.write("DENIED\n")
        else:
            run_schema(args.initialize_schema)
    except (Error, KeyError, OSError, ValueError) as exc:
        stderr.write(f"Daswork startup check failed ({type(exc).__name__})\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

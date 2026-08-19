"""``pit`` -- the point-in-time replica CLI.

One subcommand so far. ``pit tail`` arrives with DATA-715's consumer, and
``replay``/``snapshot``/``restore`` with M6 and M7; each is added when it can
actually do its job rather than as a stub that exits non-zero.

    pit initdb --db pit_base                     # schemas from the registry
    pit initdb --db pit_base --schema-dir DIR     # schemas from files
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from . import config, ddl, registry


def initdb(args: argparse.Namespace) -> int:
    """Create the sink database if needed, then its tables.

    Ordering matters and is not obvious: the database has to be created from a
    connection to a *different* database, because Postgres will not let you
    create the one you are connected to. ``pit_sink`` is that other database, and
    the sink chart makes it deliberately empty for this reason.
    """
    if args.schema_dir:
        source = f"schema files in {args.schema_dir}"
        tables = ddl.tables_from_dir(args.schema_dir)
    else:
        client = registry.Registry(base_url=args.registry or config.registry_url())
        source = f"the registry at {client.base_url}"
        topics = client.clean_topics()
        if not topics:
            print(
                f"No clean.* subjects are registered at {client.base_url}.\n"
                f"\n"
                f"That is the expected state until M4's transformer runs -- it is what\n"
                f"registers them. To build the sink schema before then, point initdb at\n"
                f"the checked-in fixtures:\n"
                f"\n"
                f"    make initdb SCHEMA_DIR=pit/tests/fixtures/clean\n",
                file=sys.stderr,
            )
            return 1
        tables = ddl.tables_from_registry(client)

    print(f"==> {len(tables)} tables from {source}")
    print(ddl.describe(tables))

    # A dry run creates nothing at all, database included -- otherwise
    # `initdb-plan` leaves behind the one thing it is least obvious it made, and
    # "print what you would do" stops being trustworthy.
    if args.dry_run:
        if database_exists(args.db):
            print(f"==> database {args.db} already there")
            connect_to = args.db
        else:
            print(f"==> would create database {args.db}")
            # Nothing to reconcile against, so plan against the maintenance
            # database: every table comes out as a create.
            connect_to = config.MAINTENANCE_DATABASE
    else:
        created = ensure_database(args.db)
        print(f"==> database {args.db}{' created' if created else ' already there'}")
        connect_to = args.db

    with psycopg.connect(config.sink_dsn(connect_to)) as conn:
        statements = ddl.ensure_schema(conn, tables, dry_run=args.dry_run)

    verb = "would run" if args.dry_run else "ran"
    print(f"==> {verb} {len(statements)} statements")
    if args.verbose or args.dry_run:
        for statement in statements:
            print(f"\n{statement};")
    return 0


def database_exists(name: str) -> bool:
    with psycopg.connect(config.sink_dsn(config.MAINTENANCE_DATABASE)) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 1 from pg_database where datname = %s", (name,))
            return cursor.fetchone() is not None


def ensure_database(name: str) -> bool:
    """``CREATE DATABASE`` if it is not there. True if this call created it.

    Autocommit because ``CREATE DATABASE`` cannot run inside a transaction
    block. The sink's application role is a superuser -- see the sink-pg chart --
    so no grant is needed.
    """
    with psycopg.connect(config.sink_dsn(config.MAINTENANCE_DATABASE), autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 1 from pg_database where datname = %s", (name,))
            if cursor.fetchone():
                return False
            # Not parameterizable: an identifier is not a value. The name comes
            # from our own CLI, and psycopg's Identifier quoting keeps it safe
            # even so.
            cursor.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
            return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pit", description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    init = subcommands.add_parser(
        "initdb",
        help="create the sink database and its tables from the clean Avro schemas",
        description=initdb.__doc__,
    )
    init.add_argument(
        "--db",
        default=config.BASE_DATABASE,
        help=f"database to create and populate (default: {config.BASE_DATABASE})",
    )
    schemas = init.add_mutually_exclusive_group()
    schemas.add_argument(
        "--registry",
        default=None,
        help="schema registry base URL (default: $PIT_REGISTRY_URL or localhost:8081)",
    )
    schemas.add_argument(
        "--schema-dir",
        type=Path,
        default=None,
        help="read clean schemas from a directory of *-key.json/*-value.json files "
        "instead of the registry",
    )
    init.add_argument(
        "--dry-run",
        action="store_true",
        help="print the statements without running them",
    )
    init.add_argument("-v", "--verbose", action="store_true", help="print each statement run")
    init.set_defaults(handler=initdb)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except (ddl.UnmappedAvroType, ddl.IncompatibleSinkSchema, registry.RegistryError) as failure:
        print(f"error: {failure}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

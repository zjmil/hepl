#! /usr/bin/env python3
import atexit
import readline
import shutil
import tempfile
from argparse import ArgumentParser
from pathlib import Path

from tableauhyperapi import (
    Connection,
    HyperException,
    HyperProcess,
    Telemetry,
    CreateMode,
)


class HeplException(Exception):
    pass


class HeplResults(list):
    def close(self):
        pass


def handle_shortcut(conn: Connection, command: str) -> HeplResults:
    command = command.strip()

    # TODO: implement proper parser
    if command == ".schemas":
        schemas = conn.catalog.get_schema_names()
        return HeplResults([schema] for schema in schemas)

    if command.startswith(".tables"):
        args = command.split()[1:]
        if not args:
            schema = "public"
        else:
            schema = args[0]
        tables = conn.catalog.get_table_names(schema)
        return HeplResults([table] for table in tables)

    if command.startswith(".schema"):
        table = command.split()[1]
        table_def = conn.catalog.get_table_definition(table)
        return HeplResults((c.name, c.type) for c in table_def.columns)

    if command == ".exit":
        raise EOFError

    if command == ".help":
        return HeplResults([["TODO"]])

    else:
        raise HeplException(f"Unknown command: {command}")


def hepl_header(conn: Connection, database: Path):
    hyper_version = conn.hyper_service_version()
    hepl_version = "0.1"  # TODO
    print(f"Hepl Version: {hepl_version}, Hyper Version: {hyper_version}")
    print('Enter ".help" for usage hints.')
    print(f"Connected to database: {database}")


def hyper_repl(conn: Connection):
    prompt = "hepl> "
    while True:
        try:
            # TODO: this currently just runs the command but should really
            #   wait until the end of a statement
            command = input(prompt)
            if command.startswith("."):
                results = handle_shortcut(conn, command)
            else:
                results = conn.execute_query(command)
            for row in results:
                print("|".join(str(x) for x in row))
            results.close()
        except (KeyboardInterrupt, EOFError):
            print()
            break
        except HeplException as e:
            print(e)
        except HyperException as e:
            print(e.main_message)


def parse_arguments():
    parser = ArgumentParser()

    parser.add_argument(
        "database", type=Path, help="Path of the database file", nargs="?", default=None
    )

    args = parser.parse_args()
    return args


def init_readline():
    histfile = Path.home() / ".hepl_history"
    try:
        readline.read_history_file(histfile)
        readline.set_history_length(1000)
    except FileNotFoundError:
        pass

    atexit.register(readline.write_history_file, histfile)


def main():
    args = parse_arguments()

    init_readline()

    temp_dir = None
    if args.database is None:
        # Connection says it can create with an empty database, but doesn't work
        # Also, using a directory instead of a named temporary file because
        # it doesn't like the file already existing
        temp_dir = Path(tempfile.mkdtemp())
        args.database = temp_dir / "repl.hyper"

    parameters = {"log_config": ""}  # don't create a log file

    try:
        with HyperProcess(
            Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=parameters
        ) as hyper:
            with Connection(
                hyper.endpoint, args.database, CreateMode.CREATE_IF_NOT_EXISTS
            ) as conn:
                hepl_header(conn, args.database)
                hyper_repl(conn)
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()

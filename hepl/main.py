#! /usr/bin/env python3
import atexit
import inspect
import readline
import shutil
import tempfile
from argparse import ArgumentParser, Namespace
from functools import lru_cache
from itertools import count
from pathlib import Path
from typing import NoReturn, Union

from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperException,
    HyperProcess,
    Result,
    Telemetry,
)


class HeplException(Exception):
    pass


# conform to the same interface as the results from the execute_query command
class HeplResults(list):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        pass


def dot_command(name: str):
    if getattr(dot_command, "commands", None) is None:
        dot_command.commands = {}

    def decorated(func):
        dot_command.commands[name] = func
        return func

    return decorated


@dot_command("schemas")
def dot_schemas(conn: Connection):
    return HeplResults((schema,) for schema in conn.catalog.get_schema_names())


@dot_command("tables")
def dot_tables(conn: Connection, schema: str = "public"):
    return HeplResults((table,) for table in conn.catalog.get_table_names(schema))


@dot_command("schema")
def dot_schema(conn: Connection, table: str):
    table_def = conn.catalog.get_table_definition(table)
    return HeplResults((c.name, c.type) for c in table_def.columns)


@dot_command("exit")
def dot_exit(_: Connection):
    raise EOFError  # simulate ^D


class DotCommandParserError(HeplException):
    pass


class DotCommandParser(ArgumentParser):
    # TODO: customize usage and error messages

    def error(self, message: str) -> NoReturn:
        raise DotCommandParserError(message)


def make_dot_command(func, parameters):
    def dot_cmd(args: Namespace, conn: Connection):
        formed_args = [getattr(args, p.name) for p in parameters]
        return func(conn, *formed_args)

    return dot_cmd


@lru_cache(maxsize=None)
def make_dot_command_parser():
    parser = DotCommandParser(prog="", add_help=False, exit_on_error=False)
    parser.set_defaults(func=None)
    subparsers = parser.add_subparsers()

    registered_commands = getattr(dot_command, "commands", {})
    for name, func in sorted(registered_commands.items()):
        subparser = subparsers.add_parser(name, add_help=False, exit_on_error=False)

        sig = inspect.signature(func)
        parameters = list(sig.parameters.values())[1:]  # skip connection
        for param in parameters:
            kwargs = {}
            if param.annotation != inspect.Parameter.empty:
                kwargs["type"] = param.annotation
            if param.default != inspect.Parameter.empty:
                kwargs["default"] = param.default
                kwargs["nargs"] = "?"

            subparser.add_argument(param.name, **kwargs)

        dot_cmd = make_dot_command(func, parameters)
        subparser.set_defaults(func=dot_cmd)

    def dot_help_cmd(*_):
        parser.print_help()
        return HeplResults()

    # add in help
    help_parser = subparsers.add_parser("help")
    help_parser.set_defaults(func=dot_help_cmd)

    return parser


def handle_dot_command(conn: Connection, command: str) -> HeplResults:
    command = command.lstrip()[1:]  # remove leading whitespace and leading .

    parser = make_dot_command_parser()
    args = parser.parse_args(command.split())
    return args.func(args, conn)


def hepl_header(conn: Connection, database: Path):
    hyper_version = conn.hyper_service_version()
    hepl_version = "0.1"  # TODO
    print(f"Hepl Version: {hepl_version}, Hyper Version: {hyper_version}")
    print('Enter ".help" for usage hints.')
    print(f"Connected to database: {database}")


def show_results(results: Union[HeplResults, Result]):
    col_sep, row_sep = "|", "\n"
    for row in results:
        print(col_sep.join(str(x) for x in row), end=row_sep)


def get_results(conn: Connection, command: str) -> Union[HeplResults, Result]:
    if command.startswith("."):
        return handle_dot_command(conn, command)
    return conn.execute_query(command)


def get_command() -> str:
    buffer = []

    main_prompt = "hepl> "
    continuation_prompt = " ...> "
    for linenum in count():
        prompt = main_prompt if linenum == 0 else continuation_prompt
        line = input(prompt)
        if not line:
            break
        buffer.append(line)

        # no semicolons needed for dot commands
        if linenum == 0 and line.lstrip().startswith("."):
            break
        # this could be better
        if line.rstrip().endswith(";"):
            break
    return "\n".join(buffer)


def hyper_repl(conn: Connection):
    while True:
        try:
            command = get_command()
            if not command:
                continue

            with get_results(conn, command) as results:
                show_results(results)

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
        "database", type=Path, help="Path of a Hyper database", nargs="?", default=None
    )
    parser.add_argument("sql", nargs="?", help="Optional SQL to execute")

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
            with Connection(hyper.endpoint, args.database, CreateMode.CREATE_IF_NOT_EXISTS) as conn:
                hepl_header(conn, args.database)
                hyper_repl(conn)
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()

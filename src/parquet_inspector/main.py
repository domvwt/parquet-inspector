"""Command line interface for inspecting parquet files."""
import ast
import textwrap
import json
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, List, Optional, Union, Callable
from types import SimpleNamespace

from pyarrow import Table  # type: ignore
from pyarrow import parquet as pq  # type: ignore
from pyarrow import json as pa_json  # type: ignore

from . import __version__


class ProcessedArgs(SimpleNamespace):
    """Processed CLI options."""

    SOURCE: Path
    func: Callable
    columns: Optional[List[str]]
    nrows: int
    filters: Optional[List[Any]]
    use_threads: bool
    memory_map: bool
    output: Optional[Path]

    def __init__(self, args: Any):
        """Initialise."""
        self.SOURCE = args.SOURCE
        self.func = args.func  # type: ignore
        self.columns = _parse_columns(getattr(args, "columns", None))
        self.nrows = getattr(args, "n", 10)
        self.filters = _parse_filters(getattr(args, "filters", None))
        self.use_threads = args.threads
        self.memory_map = args.mmap
        self.output = _parse_output(getattr(args, "output", None))


def _parse_output(output: Optional[str]) -> Optional[Path]:
    """Parse output option."""
    if output:
        path = Path(output)
        return path
    return None


def metadata(args: ProcessedArgs) -> None:
    """print file metadata"""
    print(_clean_string(str(pq.read_metadata(args.SOURCE))))


def schema(args: ProcessedArgs) -> None:
    """print data schema"""
    print(str(pq.read_schema(args.SOURCE)))


def head(args: ProcessedArgs) -> None:
    """print first n rows (default is 10)"""
    table = _read_table(args)
    n = min(args.nrows, len(table))
    for row in _take_record_dict(table, n):
        print(row)


def tail(args: ProcessedArgs) -> None:
    """print last n rows (default is 10)"""
    table = _read_table(args)
    n = min(args.nrows, len(table))
    for row in _take_record_dict(table, n, head=False):
        print(row)


def count(args: ProcessedArgs) -> None:
    """print number of rows"""
    table = _read_table(args)
    print(len(table))


def validate(args: ProcessedArgs) -> None:
    """validate file"""
    try:
        table = _read_table(args)
        table.validate()
        print("OK")
    except Exception as e:
        print(str(e).strip())
        sys.exit(1)


def to_json(args: ProcessedArgs) -> None:
    """convert parquet file to jsonl"""
    table = _read_table(args)
    if not args.output:
        args.output = args.SOURCE.with_suffix(".jsonl")
    with open(args.output, "w") as f:
        for row in _take_record_dict(table, len(table)):  # pragma: no cover
            f.write(row + "\n")


def to_parquet(args: ProcessedArgs) -> None:
    """convert jsonl file to parquet"""
    table = pa_json.read_json(args.SOURCE)
    if not args.output:
        args.output = args.SOURCE.with_suffix(".parquet")
    pq.write_table(table, args.output)


def get_parser() -> ArgumentParser:
    PROG = "pqi"
    DESCRIPTION = "parquet-inspector: cli tool for inspecting parquet files."

    parser = ArgumentParser(prog=PROG, usage=None, description=DESCRIPTION)
    parser.add_argument(
        "-v", "--version", action="version", version=__version__
    )
    parser.add_argument(
        "--threads",
        "-t",
        action="store_true",
        help="use threads for reading",
        default=False,
    )
    parser.add_argument(
        "--mmap",
        "-m",
        action="store_true",
        help="use memory mapping for reading",
        default=False,
    )

    subparsers = parser.add_subparsers(dest="subcommand")

    metadata_parser = subparsers.add_parser("metadata", help=metadata.__doc__)
    metadata_parser.set_defaults(func=metadata)

    schema_parser = subparsers.add_parser("schema", help=schema.__doc__)
    schema_parser.set_defaults(func=schema)

    head_parser = subparsers.add_parser("head", help=head.__doc__)
    head_parser.set_defaults(func=head)

    tail_parser = subparsers.add_parser("tail", help=tail.__doc__)
    tail_parser.set_defaults(func=tail)
    count_parser = subparsers.add_parser("count", help=count.__doc__)
    count_parser.set_defaults(func=count)

    validate_parser = subparsers.add_parser("validate", help=validate.__doc__)
    validate_parser.set_defaults(func=validate)

    to_json_parser = subparsers.add_parser("to-jsonl", help=to_json.__doc__)
    to_json_parser.set_defaults(func=to_json)

    to_parquet_parser = subparsers.add_parser(
        "to-parquet", help=to_parquet.__doc__
    )
    to_parquet_parser.set_defaults(func=to_parquet)

    all_parsers = [
        metadata_parser,
        schema_parser,
        head_parser,
        tail_parser,
        count_parser,
        validate_parser,
        to_json_parser,
        to_parquet_parser,
    ]

    for p in all_parsers:
        p.add_argument("SOURCE", type=Path, help="path to parquet file")

    column_reading_parsers = [
        schema_parser,
        head_parser,
        tail_parser,
        count_parser,
    ]

    for p in column_reading_parsers:
        p.add_argument(
            "--columns",
            "-c",
            type=str,
            help="comma separated list of columns to read",
            default=None,
        )

    row_reading_parsers = [head_parser, tail_parser, count_parser]

    for p in row_reading_parsers:
        p.add_argument(
            "-n", type=int, default=10, help="number of rows to print"
        )
        p.add_argument(
            "--filters",
            "-f",
            type=str,
            help="filters defined in disjunctive normal form",
            default=None,
        )

    output_parsers = [to_json_parser, to_parquet_parser]
    for p in output_parsers:
        p.add_argument(
            "--output",
            "-o",
            type=str,
            help="path to output file",
            default=None,
        )

    return parser


def _clean_string(text: str) -> str:
    """Drop first line of text and dedent the rest."""
    return textwrap.dedent("\n".join(text.splitlines()[1:]))


def _read_table(args: ProcessedArgs) -> Table:
    """Read a parquet file into a table."""
    return pq.read_table(
        args.SOURCE,
        columns=args.columns,
        use_threads=args.use_threads,
        memory_map=args.memory_map,
        filters=args.filters,
    )


def _take_record_dict(table: Table, n: int, head: bool = True) -> Table:
    """Take the first or last n rows of a table"""
    if head:
        rows = table.take(list(range(n)))
    else:
        rows = table.take(list(range(len(table) - n, len(table))))
    column_dict = rows.to_pydict()
    return (
        json.dumps(
            {key: column_dict[key][i] for key in column_dict.keys()},
            default=str,
        )
        for i in range(n)
    )


def _parse_columns(columns: Optional[str]) -> Union[List[str], None]:
    """Parse a comma-separated list of columns."""
    if not columns:
        return None
    return [c.strip() for c in columns.split(",")]


def _parse_filters(filters: Optional[str]) -> Union[List[Any], None]:
    """Parse a DNF filter string into a list of filters."""
    if not filters:
        return None
    try:
        return ast.literal_eval(filters)
    except (ValueError, SyntaxError):
        msg = (
            f"Invalid filter string: '{filters}'\n"
            "(see https://arrow.apache.org/docs/python/generated/"
            "pyarrow.parquet.read_table.html for expected format)"
        )
        print(msg)
        sys.exit(1)


def _parse_args(parser: ArgumentParser, args: Any) -> ProcessedArgs:
    """Parse arguments."""
    if args.subcommand is None:
        parser.print_help()
        sys.exit(1)
    return ProcessedArgs(args)


def _process_args(args: ProcessedArgs) -> None:
    """Process parsed arguments."""
    args.func(args)


def main() -> None:  # pragma: no cover
    """Run the CLI."""
    parser = get_parser()
    args = _parse_args(parser, parser.parse_args())
    _process_args(args)

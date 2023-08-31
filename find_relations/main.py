from datetime import timedelta
from hashlib import algorithms_available
from pathlib import Path
from sqlite3 import Connection
from time import perf_counter
from typing import Optional

from click import Choice
from click import IntRange
from click import Path as ClickPath
from click import argument
from click import group
from click import option
from click import version_option

from . import __version__
from .encode import encode_database
from .models import TableInfo
from .models import sql_types_int
from .search import Database
from .search import find_cell
from .search import find_column
from .search import find_value
from .search import find_values
from .search import print_aggregated_results
from .search import print_all_results


@group("find-relations")
@version_option(__version__)
def main():
    """
    This program converts SQLite databases into specially encoded files
    for faster search of relationships between tables.

    The first step is to encode the database using the 'encode' command.

    Once the encoded file is ready, the 'search' commands can look for specific values.
    """


# noinspection GrazieInspection
@main.command("encode", short_help="Encode a database.")
@argument("file", required=True, type=ClickPath(exists=True, dir_okay=False, resolve_path=True, path_type=Path))
@argument("output", required=False, default=None,
          type=ClickPath(exists=False, dir_okay=False, resolve_path=True, path_type=Path))
@option("--hash", "hash_algo", metavar="NAME", type=Choice(sorted(algorithms_available)), default="md5",
        show_default=True, help="The hash algorithm to use.")
@option("--sample", metavar="ROWS", type=IntRange(1), default=None,
        help="Encode a random sample of ROWS rows for each table.")
@option("--ignore-types", is_flag=True, default=False, help="Do not encode type information.")
def encode(file: Path, output: Path, hash_algo: str, ignore_types: bool, sample: Optional[int]):
    """
    Encode a SQLite database FILE into a searchable format containing the hashes of each cell's value.

    The result file will be saved as OUTPUT or as FILE.dat.

    Always available hash algorithms are: blake2b, blake2s, md5, sha1, sha224, sha256, sha384, sha3_224, sha3_256,
    sha3_384, sha3_512, sha512, shake_128, shake_256.
    """
    t1 = perf_counter()

    conn: Connection = Connection(file)
    encode_database(conn, output or file.with_suffix(file.suffix + ".dat"), hash_algo, not ignore_types, sample)

    t2 = perf_counter()

    print(f"\nConverted database in", timedelta(seconds=t2 - t1))


@main.command("search", short_help="Search an encoded database.")
@argument("file", required=1, type=ClickPath(exists=True, dir_okay=False, resolve_path=True, path_type=Path))
@option("--value", metavar="<SQL-TYPE JSON-VALUE>...", type=(Choice(list(sql_types_int.keys())), str),
        multiple=True, help="Search for specific values.")
@option("--cell", metavar="<TABLE ROW COLUMN>", type=(str, IntRange(1), IntRange(1)),
        help="Search for the value in a cell.")
@option("--column", metavar="<TABLE COLUMN>", type=(str, IntRange(1)),
        help="Search for all values in column.")
@option("--max-results", metavar="INTEGER", type=IntRange(1), help="Stop after INTEGER results.")
@option("--include-null", is_flag=True, default=False, help="Do not skip null values.")
@option("--show-all-results", is_flag=True, default=False, help="Do not aggregate results.")
def find(file: Path, value: tuple[tuple[str, str]], cell: Optional[tuple[str, int, int]],
         column: Optional[tuple[str, int]], max_results: Optional[int], include_null: bool, show_all_results: bool):
    """
    Search for specific values, cells, or columns inside an encoded FILE.

    See 'find-relations encode' for help on encoding a database.
    """
    db: Database = Database(file)
    t1: float
    t2: float
    results: list[tuple[TableInfo, list[int]]]

    t1 = perf_counter()

    if cell:
        results = find_cell(db, *cell, max_results=max_results or 0, exclude_null=not include_null)
    elif len(value) == 1:
        results = find_value(db, *value[0], max_results=max_results or 0, exclude_null=not include_null)
    elif value:
        results = find_values(db, value, max_results=max_results or 0, exclude_null=not include_null)
    elif column:
        results = find_column(db, *column, max_results=max_results or 0, exclude_null=not include_null)
    else:
        raise NotImplemented()

    t2 = perf_counter()

    print()

    if show_all_results:
        print_all_results(results)
    else:
        print_aggregated_results(results)

    print(f"\nSearched {db.data_size / db.hash_length:.0f} cells in", timedelta(seconds=t2 - t1))

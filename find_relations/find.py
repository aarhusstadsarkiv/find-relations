import asyncio
from asyncio import Queue
from functools import cached_property
from hashlib import new as new_hash
from json import loads
from pathlib import Path
from typing import Any
from typing import BinaryIO

from .encode import encode_table_column
from .models import ColInfo
from .models import Header
from .models import TableInfo
from .models import sql_types_int


class Database:
    def __init__(self, file: Path):
        self.file: Path = file
        self.handle: BinaryIO = file.open("rb")
        self.header = Header.from_handle(self.handle)
        self.tables: dict[str, TableInfo] = {t.name.lower(): t for t in self.header.tables}

    def seek(self, offset: int) -> int:
        return self.handle.seek(offset)

    def read(self, size: int) -> bytes:
        return self.handle.read(size)

    def seek_read(self, offset: int, size: int) -> bytes:
        self.seek(offset)
        return self.read(size)

    def encode_value(self, value_type: str, value: Any) -> bytes:
        return encode_table_column(value, bytes([sql_types_int[value_type]]), self.header.hash_algorithm,
                                   self.header.preserve_types)

    @cached_property
    def null_hash(self) -> bytes:
        return encode_table_column(None, bytes([0]), self.header.hash_algorithm, self.header.preserve_types)[1:]

    @cached_property
    def hash_length(self) -> int:
        return new_hash(self.header.hash_algorithm, bytes(1)).digest_size + 1

    @cached_property
    def data_start(self) -> int:
        return self.header.total_length

    @property
    def data_size(self) -> int:
        return self.file.stat().st_size - self.data_start

    def table_size(self, table: str) -> int:
        return self.tables[table.lower()].rows * len(self.tables[table.lower()].columns) * self.hash_length

    def table_offset_start(self, table: str, row: int = 0, column: int = 0) -> int:
        table_info: TableInfo = self.tables[table.lower()]
        table_index: int = [t.name for t in self.header.tables].index(table_info.name)
        offset_tables: list[TableInfo] = self.header.tables[:table_index]
        rows_offset: int = sum((self.table_size(t.name) for t in offset_tables), 0)
        rows_offset += row * len(table_info.columns) * self.hash_length
        columns_offset: int = column * self.hash_length
        return self.data_start + rows_offset + columns_offset

    def table_offset_end(self, table: str) -> int:
        return self.table_offset_start(table) + self.table_size(table) - 1


def print_all_results(results: list[tuple[TableInfo, int]]):
    for table, block in results:
        print(f"Found match in '{table.name}'"
              f" {(block // len(table.columns)) + 1}:{(block % len(table.columns)) + 1}"
              f" '{table.columns[block % len(table.columns)].name}'")

    print(
        f"{len(results)} matches found",
        f"across {len(set(t.name for [t, _] in results))} tables." if results else ""
    )


def column_from_block(table: TableInfo, block: int) -> ColInfo:
    return table.columns[block % len(table.columns)]


def print_tables_results(results: list[tuple[TableInfo, int]]):
    tables: dict[str, TableInfo] = {}
    tables_results: dict[(str, str), int] = {}

    for table, block in results:
        column: str = column_from_block(table, block).name
        tables[table.name] = table
        tables_results[(table.name, column)] = tables_results.get((table.name, column), 0) + 1

    sorter = (lambda tc: (tc[0][0], [c.name for c in tables[tc[0][0]].columns].index(tc[0][1])))

    for [table, column], count in sorted(tables_results.items(), key=sorter):
        print(f"Found {count} matches in '{table}' in column '{column}'")

    print(
        f"Found {len(results)} matches",
        f" across {len(set(t.name for [t, _] in results))} tables" if results else ""
    )


async def sort_results(output: Queue[tuple[TableInfo, int]]) -> list[tuple[TableInfo, int]]:
    results: list[tuple[TableInfo, int]] = []

    while not output.empty():
        results.append(await output.get())

    return sorted(results, key=lambda r: (r[0].name, r[1]))


async def find_value_in_region(
        file: Path, value_hash: bytes, table: TableInfo, start: int, end: int,
        output: Queue[tuple[TableInfo, int]]
):
    if output.full():
        return

    with file.open("rb") as fh:
        fh.seek(start)
        hash_length: int = len(value_hash)
        blocks: int = (end - start) // hash_length
        for block_number in range(blocks):
            if fh.read(hash_length) == value_hash:
                if output.full():
                    break
                await output.put((table, block_number))


async def find_value_parent(db: Database, value_hash: bytes, exclude: list[str], max_results: int
                            ) -> list[tuple[TableInfo, int]]:
    exclude = exclude or []
    output: Queue[tuple[TableInfo, int]] = asyncio.Queue(max_results)

    await asyncio.gather(*[
        find_value_in_region(db.file,
                             value_hash,
                             table,
                             db.table_offset_start(table.name),
                             db.table_offset_end(table.name),
                             output)
        for table in db.header.tables if table.name not in exclude
    ])

    return await sort_results(output)


def find_value(db: Database, value_type: str, value_serialised: str, *, max_results: int, exclude_null: bool
               ) -> list[tuple[TableInfo, int]]:
    value_hash: bytes

    if not db.header.preserve_types or value_type == "text":
        value_hash = db.encode_value("text", value_serialised)
    elif value_type == "blob":
        value_hash = bytes([int(value_serialised[n:n + 2], base=16) for n in range(0, len(value_serialised), 2)])
    else:
        value_hash = db.encode_value(value_type, loads(value_serialised))

    if exclude_null and value_hash == db.null_hash:
        print("Value is null", end="\n\n")
        return []

    print(f"Searching for {value_type.upper()} value {value_serialised}")
    print(*(f"{b:02x}" for b in value_hash), end="\n\n")

    return asyncio.run(find_value_parent(db, value_hash, [], max_results))


def find_cell(db: Database, table: str, row: int, column: int, *, max_results: int, exclude_null: bool
              ) -> list[tuple[TableInfo, int]]:
    value_hash: bytes = db.seek_read(db.table_offset_start(table, row - 1, column - 1), db.hash_length)

    print(f"Searching for '{table}' R{row}C{column}")

    if exclude_null and value_hash == db.null_hash:
        print("Skipping null value.", end="\n\n")
        return []

    print(*(f"{b:02x}" for b in value_hash), end="\n\n")

    return asyncio.run(find_value_parent(db, value_hash, [], max_results))


def find_column(db: Database, table: str, column: int, *, max_results: int, exclude_null: bool
                ) -> list[tuple[TableInfo, int]]:
    table_info: TableInfo = db.tables[table.lower()]

    assert 0 < column <= len(table_info.columns), f"Column {column} does not exist (max {len(table_info.columns)})"

    values_hashes: set[bytes] = set()
    results: list[tuple[TableInfo, int]] = []

    for row in range(table_info.rows):
        value_hash: bytes = db.seek_read(db.table_offset_start(table, row, column - 1), db.hash_length)

        print(f"Searching for '{table}' R{row + 1}C{column}")

        if exclude_null and value_hash == db.null_hash:
            print("Skipping null value")
            continue
        elif value_hash in values_hashes:
            print("Skipping searched value")
            continue

        print(*(f"{b:02x}" for b in value_hash))

        row_results = asyncio.run(find_value_parent(db, value_hash, [table_info.name], max_results - len(results)))

        print(f"Found {len(row_results)} matches")

        results.extend(row_results)
        values_hashes.add(value_hash)

        if len(results) >= max_results:
            break
    print()

    return results

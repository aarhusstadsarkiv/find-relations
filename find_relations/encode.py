from hashlib import new as new_hash
from pathlib import Path
from sqlite3 import Connection
from typing import Any
from typing import Generator
from typing import Optional

from orjson import dumps

from .models import ColInfo
from .models import Header
from .models import TableInfo


def get_columns(conn: Connection, table: str) -> list[ColInfo]:
    return [ColInfo(*c) for c in conn.execute(f"pragma table_info({table})").fetchall()]


# noinspection SqlNoDataSourceInspection,SqlResolve
def count_rows(conn: Connection, table: str, sample: Optional[int]) -> int:
    rows: int = conn.execute(f"select count(*) from {table}").fetchone()[0]
    return min(sample, rows) if sample else rows


def encode_table_column(value: Any, type_byte: bytes, hash_algorithm: str, preserve_types: bool) -> bytes:
    type_byte = type_byte if preserve_types else bytes([1])
    value = value if preserve_types else str(value)
    return (
            type_byte +
            new_hash(hash_algorithm, value if isinstance(value, bytes) else dumps(value, default=str)).digest()
    )


# noinspection SqlNoDataSourceInspection,SqlResolve
def encode_table_rows(conn: Connection, table: str, hash_algorithm: str, preserve_types: bool, sample: Optional[int]
                      ) -> Generator[bytes, None, None]:
    columns: list[ColInfo] = get_columns(conn, table)
    sql: str = f"select * from {table} limit {sample}" if sample else f"select * from {table}"

    return (
        encode_table_column(row[col.cid], col.byte_type, hash_algorithm, preserve_types)
        for row in conn.execute(sql)
        for col in columns
    )


# noinspection SqlNoDataSourceInspection,SqlResolve
def encode_database(conn: Connection, file: Path, hash_algorithm: str, preserve_types: bool, sample: Optional[int]):
    tables: list[str] = [t.lower() for [t] in conn.execute("select name from sqlite_master where type = 'table'")]

    header: Header = Header(hash_algorithm=hash_algorithm, tables=[], preserve_types=preserve_types)

    for i, table in enumerate(tables):
        print(f"Getting header for table {i + 1} '{table}' ...", end=" ", flush=True)
        header.tables.append(TableInfo(
            name=table,
            rows=count_rows(conn, table, sample),
            columns=get_columns(conn, table)
        ))
        print("Done")

    with file.open("wb") as fh:
        print("Writing header ...", end=" ", flush=True)
        fh.write(header.to_bytes())
        print("Done")

        for i, table_info in enumerate(header.tables):
            print(f"Writing table {i + 1} rows '{table_info.name}' ...", end=" ", flush=True)
            rows = encode_table_rows(conn, table_info.name, hash_algorithm, preserve_types, sample)
            for j, row in enumerate(rows):
                fh.write(row)
                if (j % 1000) == 0:
                    print(f"\rWriting table {i + 1} rows '{table_info.name}' ... {j / table_info.rows:02.01f}%",
                          end=" ", flush=True)
            print(f"\rWriting table {i + 1} rows '{table_info.name}' ... Done  ")

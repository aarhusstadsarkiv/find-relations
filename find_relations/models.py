from dataclasses import asdict
from dataclasses import dataclass
from struct import calcsize
from struct import pack
from struct import unpack
from typing import Any
from typing import BinaryIO
from typing import Optional

from orjson import dumps
from orjson import loads

sql_types_int: dict[str, int] = {
    "integer": 0,
    "text": 1,
    "blob": 2,
    "real": 3,
    "numeric": 4,
}


@dataclass
class ColInfo:
    cid: int
    name: str
    type: str
    notnull: bool
    dflt_value: Any | None
    pk: bool

    @property
    def byte_type(self) -> bytes:
        return bytes([sql_types_int[self.type.lower()]])


@dataclass
class TableInfo:
    name: str
    rows: int
    columns: list[ColInfo]


@dataclass
class Header:
    hash_algorithm: str
    preserve_types: bool
    tables: list[TableInfo]
    bytes_data: Optional[bytes] = None

    @property
    def length(self) -> int:
        return len(self.bytes_data or self.bytes)

    @property
    def total_length(self) -> int:
        return calcsize("<L") + self.length

    @property
    def bytes(self) -> bytes:
        return dumps(asdict(self))

    @classmethod
    def from_handle(cls, handle: BinaryIO) -> 'Header':
        handle.seek(0)
        length: int = unpack("<L", handle.read(4))[0]
        bytes_data: bytes = handle.read(length)
        data: dict = loads(bytes_data)
        tables: list[TableInfo] = [
            TableInfo(name=t["name"], rows=t["rows"], columns=[ColInfo(**c) for c in t["columns"]])
            for t in data["tables"]
        ]

        return Header(hash_algorithm=data["hash_algorithm"], preserve_types=data["preserve_types"],
                      tables=tables, bytes_data=bytes_data)

    def to_bytes(self):
        return pack("<L", self.length) + self.bytes

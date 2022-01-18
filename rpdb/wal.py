from collections.abc import Collection
from typing import Iterator

from proto.rpdb import WAL
from rpdb.types import Write


class WriteAheadLog(Collection):
    def __init__(self, wal_file_location: str) -> None:
        self.wal_file_location = wal_file_location
        self.writer = open(wal_file_location, "ab")

    def __iter__(self) -> Iterator[Write]:
        with open(self.wal_file_location, "rb") as replay:
            wal = WAL()
            for i in wal.parse(replay.read()).entries:
                yield Write.from_wal_entry(i)

    def __len__(self) -> int:
        return len(list(iter(self)))

    def __contains__(self, __x: object) -> bool:
        if isinstance(__x, Write):
            return __x in iter(self)
        return False

    def __del__(self):
        self.close()

    def append(self, op: Write):
        wal = WAL()
        wal.entries.append(op.to_wal_entry())
        self.writer.write(bytes(wal))
        self.writer.flush()

    def clear(self):
        self.writer.truncate(0)

    def close(self):
        self.writer.close()

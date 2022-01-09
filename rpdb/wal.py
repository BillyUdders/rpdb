import time
import zlib
from collections.abc import Collection
from contextlib import contextmanager
from typing import Iterator

from proto.rpdb import WAL, WALEntry, WALEntryOpType
from rpdb.operations import Write, WriterOps

OP_DICT = {
    WALEntryOpType.BEGIN: WriterOps.BEGIN,
    WALEntryOpType.SET: WriterOps.SET,
    WALEntryOpType.UNSET: WriterOps.UNSET,
    WALEntryOpType.COMMIT: WriterOps.COMMIT,
    WALEntryOpType.ROLLBACK: WriterOps.ROLLBACK,
}
REVERSE_OP_DICT = {v: k for k, v in OP_DICT.items()}


class WriteAheadLog(Collection):
    def __init__(self, wal_file_location: str) -> None:
        self.wal_file_location = wal_file_location
        self.writer = open(wal_file_location, "ab")

    @contextmanager
    def get_entries(self) -> Iterator[map]:
        with open(self.wal_file_location, "rb") as replay:
            wal = WAL()
            yield map(create_write, wal.parse(replay.read()).entries)

    def __iter__(self) -> Iterator[Write]:
        with self.get_entries() as entries:
            return entries

    def __len__(self) -> int:
        with self.get_entries() as entries:
            return len(list(entries))

    def __contains__(self, __x: object) -> bool:
        if isinstance(__x, Write):
            with self.get_entries() as entries:
                return __x in entries
        return False

    def __del__(self):
        self.close()

    def append(self, op: Write):
        wal = WAL()
        wal.entries.append(create_wal_entry(op))
        self.writer.write(bytes(wal))
        self.writer.flush()

    def clear(self):
        self.writer.truncate(0)

    def close(self):
        self.writer.close()


# Serialize
def create_wal_entry(op: Write) -> WALEntry:
    entry = WALEntry(
        timestamp=time.time_ns(),
        op_type=REVERSE_OP_DICT[op.op_type],
        key=op.key or "",
        value=op.value or "",
    )
    entry.crc32 = zlib.crc32(bytes(entry))
    return entry


# Deserialize
def create_write(e: WALEntry) -> Write:
    return Write(OP_DICT[e.op_type], e.key, e.value)

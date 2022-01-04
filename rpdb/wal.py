import contextlib
import io
import time
import zlib
from collections.abc import Collection
from typing import Iterator

from proto.rpdb import WAL, WALEntry, WALEntryOpType
from rpdb.operations import Write, WriterOps

OP_DICT = {
    WALEntryOpType.BEGIN: WriterOps.BEGIN,  # type: ignore
    WALEntryOpType.SET: WriterOps.SET,  # type: ignore
    WALEntryOpType.UNSET: WriterOps.UNSET,  # type: ignore
    WALEntryOpType.COMMIT: WriterOps.COMMIT,  # type: ignore
    WALEntryOpType.ROLLBACK: WriterOps.ROLLBACK,  # type: ignore
}
REVERSE_OP_DICT = {v: k for k, v in OP_DICT.items()}


class WriteAheadLog(Collection):
    def __init__(self, wal_file_location: str) -> None:
        self.wal_file_location = wal_file_location
        self.writer = open(wal_file_location, "+ab")

    @contextlib.contextmanager
    def get_entries(self):
        wal = WAL()
        self.writer.seek(0)
        yield wal.parse(self.writer.read()).entries
        self.writer.seek(0, io.SEEK_END)

    def __iter__(self) -> Iterator[Write]:
        with self.get_entries() as entries:
            for e in entries:
                yield create_write(e)

    def __len__(self) -> int:
        with self.get_entries() as entries:
            return len(entries)

    def __contains__(self, __x: object) -> bool:
        if isinstance(__x, Write):
            with self.get_entries() as entries:
                return __x in map(create_write, entries)
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

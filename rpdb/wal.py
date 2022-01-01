import time
import zlib
from collections.abc import Collection
from typing import Iterator

from proto.WAL_pb2 import WAL, WALEntry
from rpdb.operations import Write, WriterOps

OP_DICT = {
    WALEntry.OpType.BEGIN: WriterOps.BEGIN,  # type: ignore
    WALEntry.OpType.SET: WriterOps.SET,  # type: ignore
    WALEntry.OpType.UNSET: WriterOps.UNSET,  # type: ignore
    WALEntry.OpType.COMMIT: WriterOps.COMMIT,  # type: ignore
    WALEntry.OpType.ROLLBACK: WriterOps.ROLLBACK,  # type: ignore
}
REVERSE_OP_DICT = {v: k for k, v in OP_DICT.items()}


class WriteAheadLog(Collection):
    def __init__(self, wal_file_location: str) -> None:
        self.writer = open(wal_file_location, "+ab")
        self.store = WAL()

    def __len__(self) -> int:
        pass

    def __iter__(self) -> Iterator[Write]:
        self.writer.seek(0)
        self.store.ParseFromString(self.writer.read())
        for e in self.store.entries:
            yield create_write(e)

    def __contains__(self, __x: object) -> bool:
        pass

    def __del__(self):
        self.close()

    def append(self, op: Write):
        create_wal_entry(self.store.entries.add(), op)
        self.writer.write(self.store.SerializeToString())
        self.writer.flush()
        self.store.Clear()

    def clear(self):
        self.writer.truncate(0)
        self.store.Clear()

    def close(self):
        self.writer.close()


# Serialize
def create_wal_entry(entry, op: Write):
    entry.timestamp = time.time_ns()
    if op.key is not None:
        entry.key = op.key
    if op.value is not None:
        entry.value = op.value
    entry.op_type = REVERSE_OP_DICT[op.op_type]
    entry.crc32 = zlib.crc32(entry.SerializeToString())


# Deserialize
def create_write(e) -> Write:
    val = e.value if e.HasField("value") else None
    return Write(OP_DICT[e.op_type], e.key, val)

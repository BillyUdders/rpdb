import time
from typing import Iterator

from proto.WAL_pb2 import WAL, WALEntry
from rpdb.operations import Operation, OpType

OP_DICT = {
    WALEntry.OpType.BEGIN: OpType.BEGIN,  # type: ignore
    WALEntry.OpType.SET: OpType.SET,  # type: ignore
    WALEntry.OpType.UNSET: OpType.UNSET,  # type: ignore
    WALEntry.OpType.COMMIT: OpType.COMMIT,  # type: ignore
    WALEntry.OpType.ROLLBACK: OpType.ROLLBACK,  # type: ignore
}


class WriteAheadLog:
    def __init__(self, wal_file_location: str) -> None:
        self.writer = open(wal_file_location, "+ab")
        self.store = WAL()

    def __del__(self):
        self.writer.close()

    def append(self, op: Operation):
        create_entry(self.store.entries.add(), op)
        self.writer.write(self.store.SerializeToString())
        self.writer.flush()
        self.store.Clear()

    def read_all(self) -> Iterator[Operation]:
        self.writer.seek(0)
        self.store.ParseFromString(self.writer.read())
        yield from create_operations(self.store.entries)

    def clear(self):
        self.writer.truncate(0)

    def close(self):
        self.writer.close()


def create_operations(entries) -> Iterator[Operation]:
    for e in entries:
        yield Operation(OP_DICT[e.op_type], e.key, e.value)


def create_entry(entry, op: Operation):
    entry.timestamp = time.time_ns()
    entry.key = op.key
    entry.value = str(op.value)
    entry.op_type = {v: k for k, v in OP_DICT.items()}[op.op_type]

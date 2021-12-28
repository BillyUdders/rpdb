import time
from typing import Iterator, Tuple

from proto.WAL_pb2 import WAL, WALEntry
from rpdb.operations import Operation, WriterOps


class WriteAheadLog:
    def __init__(self, wal_file_location: str) -> None:
        self.writer = open(wal_file_location, "+ab")
        self.store = WAL()

    def __del__(self):
        self.writer.close()

    def append(self, op: Operation):
        self.__create_entry(self.store.add(), op)
        self.writer.write(self.store.SerializeToString())
        self.writer.flush()

    @staticmethod
    def __create_entry(entry, op: Operation):
        entry.timestamp = time.time_ns()
        entry.key = op.key
        entry.value = op.value

        if op.op_type == WriterOps.BEGIN:
            entry.op_type = WALEntry.OpType.BEGIN  # type: ignore
        if op.op_type == WriterOps.SET:
            entry.op_type = WALEntry.OpType.SET  # type: ignore
        if op.op_type == WriterOps.UNSET:
            entry.op_type = WALEntry.OpType.UNSET  # type: ignore
        if op.op_type == WriterOps.COMMIT:
            entry.op_type = WALEntry.OpType.COMMIT  # type: ignore
        if op.op_type == WriterOps.ROLLBACK:
            entry.op_type = WALEntry.OpType.ROLLBACK  # type: ignore

    def restore_all(self) -> Iterator[Tuple[str, str]]:
        self.writer.seek(0)
        yield from self._read_pairs()

    def _read_pairs(self) -> Iterator[Tuple[str, str]]:
        pass

    def clear(self):
        self.writer.truncate(0)

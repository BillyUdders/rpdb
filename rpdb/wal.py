import time
from typing import Iterator, Tuple

from proto.WAL_pb2 import WAL as Store
from proto.WAL_pb2 import WALEntry as Entry
from rpdb.operations import Operation


class WAL:
    def __init__(self, wal_file_location: str) -> None:
        self.writer = open(wal_file_location, "+ab")
        self.store = Store()

    def __del__(self):
        self.writer.close()

    def append(self, op: Operation):
        entry = Entry()
        entry.timestamp = time.time_ns()

    def restore_all(self) -> Iterator[Tuple[str, str]]:
        self.writer.seek(0)
        yield from self._read_pairs()

    def _read_pairs(self) -> Iterator[Tuple[str, str]]:
        pass

    def clear(self):
        self.writer.truncate(0)

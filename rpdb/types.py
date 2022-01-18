import time
import zlib
from enum import Enum, auto
from typing import List, Optional

from attrs import define

from proto.rpdb import WALEntry, WALEntryOpType


class ReaderOps(Enum):
    GET = auto()
    EXISTS = auto()


class WriterOps(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    ROLLBACK = auto()
    COMMIT = auto()


OP_DICT = {
    WALEntryOpType.BEGIN: WriterOps.BEGIN,
    WALEntryOpType.SET: WriterOps.SET,
    WALEntryOpType.UNSET: WriterOps.UNSET,
    WALEntryOpType.COMMIT: WriterOps.COMMIT,
    WALEntryOpType.ROLLBACK: WriterOps.ROLLBACK,
}
REVERSE_OP_DICT = {v: k for k, v in OP_DICT.items()}


@define
class Read:
    op_type: ReaderOps
    key: str


@define
class Write:
    op_type: WriterOps
    key: Optional[str] = None
    value: Optional[str] = ""

    # Serialize
    @staticmethod
    def to_wal_entry(e: "Write") -> WALEntry:
        entry = WALEntry(
            timestamp=time.time_ns(),
            op_type=REVERSE_OP_DICT[e.op_type],
            key=e.key or "",
        )
        entry.value = e.value or ""
        entry.crc32 = zlib.crc32(bytes(entry))
        return entry

    # Deserialize
    @staticmethod
    def from_wal_entry(e: WALEntry) -> "Write":
        return Write(OP_DICT[e.op_type], e.key, e.value)


@define
class Transaction:
    operations: List[Write] = []

    def do(self, op_type: WriterOps, key=None, value=None):
        self.operations.append(Write(op_type, key, value))

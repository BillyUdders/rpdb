from enum import Enum, auto
from typing import List, Optional

from attrs import define


class ReaderOps(Enum):
    GET = auto()
    EXISTS = auto()


class WriterOps(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    ROLLBACK = auto()
    COMMIT = auto()


@define
class Read:
    op_type: ReaderOps
    key: str


@define
class Write:
    op_type: WriterOps
    key: Optional[str] = None
    value: Optional[str] = ""


@define
class Transaction:
    operations: List[Write] = []

    def do(self, op_type: WriterOps, key=None, value=None):
        self.operations.append(Write(op_type, key, value))

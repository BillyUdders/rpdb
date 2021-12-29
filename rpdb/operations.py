from enum import Enum, auto
from typing import NamedTuple, Optional


class ReaderOps(Enum):
    GET = auto()
    EXISTS = auto()


class WriterOps(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    ROLLBACK = auto()
    COMMIT = auto()


class Read(NamedTuple):
    op_type: ReaderOps
    key: str


class Write(NamedTuple):
    op_type: WriterOps
    key: str
    value: Optional[str] = None

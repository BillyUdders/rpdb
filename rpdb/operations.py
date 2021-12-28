from enum import Enum, auto
from typing import NamedTuple, Optional, Union


class ReaderOps(Enum):
    GET = auto()
    EXISTS = auto()


class WriterOps(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    ROLLBACK = auto()
    COMMIT = auto()


OpType = Union[ReaderOps, WriterOps]


class Operation(NamedTuple):
    op_type: OpType
    key: str
    value: Optional[object]

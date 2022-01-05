from enum import Enum, auto
from typing import Optional

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

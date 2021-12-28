import contextlib
import uuid
from enum import Enum, auto
from typing import Any, Callable, Dict, NamedTuple, NewType, Optional

from sortedcontainers import SortedDict as MemTable

TransactionID = NewType("TransactionID", uuid.UUID)


class OperationType(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    GET = auto()
    EXISTS = auto()
    ROLLBACK = auto()
    COMMIT = auto()


class Operation(NamedTuple):
    operation_type: OperationType
    key: object
    value: Optional[object]


class Transaction(list):
    def __init__(self):
        super().__init__()
        self.id: TransactionID = TransactionID(uuid.uuid4())
        self.do(OperationType.BEGIN)

    def do(self, op_type: OperationType, key=None, value=None):
        self.append(Operation(op_type, key, value))

    def __repr__(self) -> str:
        return f"Transaction(ops={self}, id={self.id})"


class State:
    def __init__(self) -> None:
        self.OPERATIONS: Dict[OperationType, Callable] = {
            OperationType.BEGIN: self.__begin,
            OperationType.SET: self.__set,
            OperationType.UNSET: self.__unset,
            OperationType.EXISTS: self.__exists,
            OperationType.ROLLBACK: self.__rollback,
            OperationType.COMMIT: self.__commit,
        }

        self._memtable = MemTable()
        self._sstables = MemTable(key=lambda x: str(x.path))
        self.wal: Dict[TransactionID, Operation] = {}

    def operate(self, tx: Transaction) -> Optional[Any]:
        for op in tx:
            self.OPERATIONS[op.operation_type](tx.id, op)
        return {"FIXME"}

    def __begin(self, tx_id: TransactionID, op: Operation) -> None:
        self.wal[tx_id] = op

    def __set(self, tx_id: TransactionID, op: Operation) -> None:
        pass

    def __unset(self, tx_id: TransactionID, op: Operation) -> None:
        pass

    def __exists(self, tx_id: TransactionID, op: Operation) -> None:
        pass

    def __rollback(self, tx_id: TransactionID, op: Operation) -> None:
        pass

    def __commit(self, tx_id: TransactionID, op: Operation) -> None:
        pass


class SimpleDB:
    def __init__(self):
        self.__state: State = State()
        self.live_txs: list[Transaction] = []

    @contextlib.contextmanager
    def transaction(self):
        tx = Transaction()
        self.live_txs.append(tx)
        yield tx
        tx.do(OperationType.COMMIT)
        self.__state.operate(tx)
        self.live_txs.remove(tx)

    @property
    def state(self):
        return self.__state

    def set(self, key, value):
        self.__exec(OperationType.SET, key, value)

    def unset(self, key):
        self.__exec(OperationType.UNSET, key)

    def get(self, key):
        return self.__exec(OperationType.GET, key)

    def exists(self, key):
        return self.__exec(OperationType.EXISTS, key)

    def __exec(self, operation, key, value=None):
        if self.live_txs:
            self.live_txs[-1].do(operation, key, value)
        else:
            with self.transaction() as tx:
                tx.do(operation, key, value)

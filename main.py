import contextlib
from enum import Enum, auto
from typing import Any, Dict, List, NamedTuple


class OperationType(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    GET = auto()
    ROLLBACK = auto()
    COMMIT = auto()


class Operation(NamedTuple):
    operation_type: OperationType
    key: object
    value: object


class State(dict):
    # Replace Any with Callable when we implement
    OPERATIONS: Dict[OperationType, Any] = {
        OperationType.BEGIN: None,
        OperationType.SET: None,
        OperationType.UNSET: None,
        OperationType.ROLLBACK: None,
        OperationType.COMMIT: None,
    }

    def __init__(self) -> None:
        super().__init__()
        self.wal: List[Transaction] = []

    def operate(self, tx: "Transaction"):
        try:
            for op in tx:
                self.OPERATIONS[op.operation_type](op)
        except Exception as e:
            print(e)


class Transaction(list):
    def __init__(self):
        super().__init__()
        self.do(OperationType.BEGIN)

    def do(self, op_type: OperationType, key=None, value=None):
        self.append(Operation(op_type, key, value))

    def __repr__(self) -> str:
        return f"Transaction(ops={self})"


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
        return self.__state[key]

    def exists(self, key):
        return key in self.__state

    def __exec(self, operation, key, value=None):
        if self.live_txs:
            self.live_txs[-1].do(operation, key, value)
        else:
            with self.transaction() as tx:
                tx.do(operation, key, value)

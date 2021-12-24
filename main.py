import contextlib
from enum import Enum, auto
from typing import NamedTuple


class OperationType(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    GET = auto()
    COMMIT = auto()


class Operation(NamedTuple):
    operation_type: OperationType
    key: object
    value: object


class State(dict):
    def commit(self, tx: "Transaction"):
        try:
            if tx.is_rolled_back:
                return
            for op in tx:
                if op.operation_type == OperationType.SET:
                    self[op.key] = op.value
                elif op.operation_type == OperationType.UNSET:
                    del self[op.key]
                elif op.operation_type == OperationType.GET:
                    return self[op.key]
        except Exception as e:
            print(e)
        finally:
            tx.do(OperationType.COMMIT)


class Transaction(list):
    def __init__(self):
        super().__init__()
        self._rolled_back: bool = False
        self.do(OperationType.BEGIN)

    def do(self, op: OperationType, key=None, value=None):
        self.append(Operation(op, key, value))

    @property
    def is_rolled_back(self):
        return self._rolled_back

    def rollback(self):
        self._rolled_back = True

    def __repr__(self) -> str:
        return f"Transaction(_rolled_back={self._rolled_back}, ops={self})"


class SimpleDB:
    def __init__(self):
        self.__state: State = State()
        self.live_txs: list[Transaction] = []
        self.tx_log: list[Transaction] = []

    @contextlib.contextmanager
    def transaction(self):
        tx = Transaction()
        self.live_txs.append(tx)
        yield tx
        self.__state.commit(tx)
        self.tx_log.append(tx)
        self.live_txs.remove(tx)

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

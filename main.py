import contextlib
from enum import Enum, auto
from typing import NamedTuple


class OperationType(Enum):
    BEGIN = auto()
    SET = auto()
    UNSET = auto()
    GET = auto()
    COMMIT = auto()
    TEST = auto()


class Operation(NamedTuple):
    operation_type: OperationType
    key: object
    value: object


class Transaction:
    def __init__(self):
        self._rolled_back: bool = False
        self.operations: list[Operation] = []
        self.do(OperationType.BEGIN)

    def do(self, op: OperationType, key=None, value=None):
        self.operations.append(Operation(op, key, value))

    @property
    def is_rolled_back(self):
        return self._rolled_back

    def rollback(self):
        self._rolled_back = True

    def __repr__(self) -> str:
        return (
            f"Transaction(_rolled_back={self._rolled_back},"
            f" operations={self.operations})"
        )


class SimpleDB:
    def __init__(self):
        self.state: dict = {}
        self.live_transactions: list[Transaction] = []
        self.transaction_log: list[Transaction] = []

    @contextlib.contextmanager
    def transaction(self):
        tx = Transaction()
        self.live_transactions.append(tx)
        yield tx
        self.__commit(tx)
        self.live_transactions.remove(tx)

    def set(self, key, value):
        self.__transact(OperationType.SET, key, value)

    def unset(self, key):
        self.__transact(OperationType.UNSET, key)

    def get(self, key):
        return self.state[key]

    def exists(self, key):
        return key in self.state

    def __transact(self, operation, key, value=None):
        if self.live_transactions:
            tx = self.live_transactions[-1]
            tx.do(operation, key, value)
        else:
            with self.transaction() as tx:
                tx.do(operation, key, value)

    def __commit(self, tx: Transaction):
        try:
            if tx.is_rolled_back:
                return
            for op in tx.operations:
                if op.operation_type == OperationType.SET:
                    self.state[op.key] = op.value
                elif op.operation_type == OperationType.UNSET:
                    del self.state[op.key]
        except Exception as e:
            print(e)
        finally:
            self.transaction_log.append(tx)
            tx.do(OperationType.COMMIT)

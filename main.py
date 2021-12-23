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
    def __init__(self, state=None) -> None:
        super().__init__()
        if state is None:
            state = {}
        self.__state = state

    @property
    def state(self) -> dict:
        return self.__state

    def commit(self, tx: "Transaction"):
        try:
            if tx.is_rolled_back:
                return
            for op in tx.operations:
                if op.operation_type == OperationType.SET:
                    self.__state[op.key] = op.value
                elif op.operation_type == OperationType.UNSET:
                    del self.__state[op.key]
        except Exception as e:
            print(e)
        finally:
            tx.do(OperationType.COMMIT)


class Transaction:
    def __init__(self):
        self._rolled_back: bool = False
        self.operations: list[Operation] = []
        self.temp_state: State = State()
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
            f"Transaction(_rolled_back={self._rolled_back}, "
            f"operations={self.operations})"
        )


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
        self._do_on_current_tx(OperationType.SET, key, value)

    def unset(self, key):
        self._do_on_current_tx(OperationType.UNSET, key)

    def get(self, key):
        return self.__state.state[key]

    def exists(self, key):
        return key in self.__state.state

    def _do_on_current_tx(self, operation, key, value=None):
        if self.live_txs:
            self.live_txs[-1].do(operation, key, value)
        else:
            with self.transaction() as tx:
                tx.do(operation, key, value)

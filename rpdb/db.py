import contextlib
from typing import Any, Callable, Dict, Optional

from sortedcontainers import SortedDict

from rpdb.operations import Operation, OpType, ReaderOps, WriterOps
from rpdb.transaction import Transaction
from rpdb.wal import WAL


class State:
    def __init__(self) -> None:
        self.OPERATIONS: Dict[OpType, Callable] = {
            WriterOps.BEGIN: self.__begin,
            WriterOps.SET: self.__set,
            WriterOps.UNSET: self.__unset,
            WriterOps.ROLLBACK: self.__rollback,
            WriterOps.COMMIT: self.__commit,
            ReaderOps.EXISTS: self.__exists,
            ReaderOps.GET: self.__get,
        }

        self._memtable: SortedDict = SortedDict()
        self._sstables: SortedDict = SortedDict(key=lambda x: str(x.path))
        self.wal: WAL = WAL("/tmp/wal.dat")

    def operate(self, tx: Transaction) -> Optional[Any]:
        res = None
        for op in tx:
            if val := self.OPERATIONS[op.op_type](tx.id, op):
                res = val

        return res

    def __begin(self, op: Operation) -> None:
        self.wal.append(op)

    def __set(self, tx: Transaction, op: Operation) -> None:
        pass

    def __unset(self, tx: Transaction, op: Operation) -> None:
        pass

    def __rollback(self, tx: Transaction, op: Operation) -> None:
        pass

    def __commit(self, tx: Transaction, op: Operation) -> None:
        pass

    def __get(self, tx: Transaction, op: Operation) -> None:
        pass

    def __exists(self, tx: Transaction, op: Operation) -> None:
        pass


class DB:
    def __init__(self):
        self.__state: State = State()
        self.live_txs: list[Transaction] = []

    @contextlib.contextmanager
    def transaction(self):
        tx = Transaction()
        self.live_txs.append(tx)
        yield tx
        tx.do(WriterOps.COMMIT)
        self.__state.operate(tx)
        self.live_txs.remove(tx)

    @property
    def state(self):
        return self.__state

    def set(self, key, value):
        self.__exec(WriterOps.SET, key, value)

    def unset(self, key):
        self.__exec(WriterOps.UNSET, key)

    def get(self, key):
        return self.__exec(ReaderOps.GET, key)

    def exists(self, key):
        return self.__exec(ReaderOps.EXISTS, key)

    def __exec(self, operation, key, value=None):
        if self.live_txs:
            self.live_txs[-1].do(operation, key, value)
        else:
            with self.transaction() as tx:
                tx.do(operation, key, value)

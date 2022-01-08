from collections import MutableMapping
from contextlib import contextmanager
from typing import Iterator, List

from sortedcontainers import SortedDict

from rpdb.operations import Write, WriterOps
from rpdb.transaction import Transaction
from rpdb.wal import WriteAheadLog


class DB(MutableMapping):
    def __init__(
        self, write_ahead_log_file: str = "/tmp/wal.dat", memtable_items_limit=512
    ):
        self.memtable_items_limit = memtable_items_limit
        self.wal: WriteAheadLog = WriteAheadLog(write_ahead_log_file)
        self._memtable: SortedDict = SortedDict()
        self._sstables: SortedDict = SortedDict(key=lambda x: str(x.path))
        self.tx_log: List[Transaction] = []

    @contextmanager
    def transaction(self):
        tx = Transaction()
        tx.do(WriterOps.BEGIN)
        yield tx
        tx.do(WriterOps.COMMIT)
        for op in tx.operations:
            self.__write(op)
        self.tx_log.append(tx)

    def __getitem__(self, k):
        return self._memtable[k]

    def __setitem__(self, k, v) -> None:
        with self.transaction() as tx:
            tx.do(WriterOps.SET, k, v)

    def __delitem__(self, v) -> None:
        with self.transaction() as tx:
            tx.do(WriterOps.UNSET, v)

    def __len__(self) -> int:
        return len(self._memtable)

    def __iter__(self) -> Iterator:
        return iter(self._memtable)

    def exists(self, key):
        return key in self._memtable

    def __write(self, op: Write):
        self.wal.append(op)
        if op.op_type == WriterOps.SET:
            self._memtable[op.key] = op.value
        elif op.op_type == WriterOps.UNSET:
            del self._memtable[op.key]

        if self._memtable_limit_reached():
            self._dump_memtable_to_sstable()

    def _memtable_limit_reached(self):
        return len(self) >= self.memtable_items_limit

    def _dump_memtable_to_sstable(self):
        self._memtable = SortedDict()

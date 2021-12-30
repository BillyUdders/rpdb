import contextlib

from sortedcontainers import SortedDict

from rpdb.operations import ReaderOps, Write, WriterOps
from rpdb.transaction import Transaction
from rpdb.wal import WriteAheadLog


class DB:
    def __init__(self, write_ahead_log_file: str = "/tmp/wal.dat"):
        self._memtable: SortedDict = SortedDict()
        self._sstables: SortedDict = SortedDict(key=lambda x: str(x.path))
        self.wal: WriteAheadLog = WriteAheadLog(write_ahead_log_file)

    @contextlib.contextmanager
    def transaction(self):
        tx = Transaction()
        tx.do(WriterOps.BEGIN)
        yield tx
        tx.do(WriterOps.COMMIT)
        for op in tx.operations:
            self.__write(op)

    def set(self, key, value):
        with self.transaction() as tx:
            tx.do(WriterOps.SET, key, value)

    def unset(self, key):
        with self.transaction() as tx:
            tx.do(WriterOps.UNSET, key)

    def get(self, key):
        return self.__read(ReaderOps.GET, key)

    def exists(self, key):
        return self.__read(ReaderOps.EXISTS, key)

    def __write(self, op: Write):
        self.wal.append(op)
        if op.op_type == WriterOps.COMMIT:
            self.__commit()

    def __commit(self):
        for wal_write in self.wal.read():
            if wal_write.op_type == WriterOps.SET:
                self._memtable[wal_write.key] = wal_write.value
            elif wal_write.op_type == WriterOps.UNSET:
                del self._memtable[wal_write.key]
        # self.wal.clear()

    def __read(self, op: ReaderOps, key: str):
        if op == ReaderOps.GET:
            return self._memtable[key]
        elif op == ReaderOps.EXISTS:
            return key in self._memtable

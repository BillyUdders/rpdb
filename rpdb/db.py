import contextlib

from sortedcontainers import SortedDict

from rpdb.operations import ReaderOps, WriterOps
from rpdb.transaction import Transaction
from rpdb.wal import WriteAheadLog


class DB:
    def __init__(self, write_ahead_log_file: str = "/tmp/wal.dat"):
        self.live_txs: list[Transaction] = []
        self._memtable: SortedDict = SortedDict()
        self._sstables: SortedDict = SortedDict(key=lambda x: str(x.path))
        self.wal: WriteAheadLog = WriteAheadLog(write_ahead_log_file)

    @contextlib.contextmanager
    def transaction(self):
        tx = Transaction()
        self.live_txs.append(tx)
        yield tx
        tx.do(WriterOps.COMMIT)
        self.live_txs.remove(tx)

    def set(self, key, value):
        self.__write(WriterOps.SET, key, value)

    def unset(self, key):
        self.__write(WriterOps.SET, key)

    def get(self, key):
        return self.__read(ReaderOps.GET, key)

    def exists(self, key):
        return self.__read(ReaderOps.EXISTS, key)

    def __write(self, op: WriterOps, key: str, value: object = None):
        pass

    def __read(self, op: ReaderOps, key: str):
        pass

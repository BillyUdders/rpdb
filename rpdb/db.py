from contextlib import contextmanager
from typing import List

from rpdb.operations import Write, WriterOps
from rpdb.storage import Storage
from rpdb.transaction import Transaction
from rpdb.wal import WriteAheadLog


class DB:
    def __init__(self, write_ahead_log_file: str = "/tmp/wal.dat"):
        self.wal: WriteAheadLog = WriteAheadLog(write_ahead_log_file)
        self.storage: Storage = Storage()
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

    def set(self, key, value):
        with self.transaction() as tx:
            tx.do(WriterOps.SET, key, value)

    def unset(self, key):
        with self.transaction() as tx:
            tx.do(WriterOps.UNSET, key)

    def get(self, key):
        return self.storage[key]

    def exists(self, key):
        return key in self.storage

    def __write(self, op: Write):
        self.wal.append(op)
        if op.op_type == WriterOps.COMMIT:
            self.__commit()

    def __commit(self):
        for write in self.wal:
            if write.op_type == WriterOps.SET:
                self.storage[write.key] = write.value
            elif write.op_type == WriterOps.UNSET:
                del self.storage[write.key]
        self.wal.clear()

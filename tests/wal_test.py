import unittest

from rpdb.operations import Operation, WriterOps
from rpdb.wal import WriteAheadLog


class WALTestCase(unittest.TestCase):
    def test_opens_writes_closes(self):
        wal = WriteAheadLog("/tmp/wal.dat")
        self.assertFalse(wal.writer.closed)
        wal.append(Operation(WriterOps.SET, "a", 23))
        wal.close()
        self.assertTrue(wal.writer.closed)

    def test_opens_writes_closes_init_reads_closes(self):
        # open
        wal = WriteAheadLog("/tmp/wal.dat")
        wal.clear()
        self.assertFalse(wal.writer.closed)

        # write and flush
        op = Operation(WriterOps.SET, "a", 23)
        wal.append(op)
        wal.append(op)

        # close
        wal.close()
        self.assertTrue(wal.writer.closed)

        # re-init
        wal = WriteAheadLog("/tmp/wal.dat")
        self.assertFalse(wal.writer.closed)

        # read
        self.assertEquals(list(wal.read_all()), [op, op])

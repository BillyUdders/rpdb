import unittest

from rpdb.operations import Write, WriterOps
from rpdb.wal import WriteAheadLog

WAL_FILE_LOCATION = "/tmp/wal.dat"


class WALTestCase(unittest.TestCase):
    def test_opens_writes_closes(self):
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(wal.writer.closed)
        wal.append(Write(WriterOps.SET, "a", 23))
        wal.close()
        self.assertTrue(wal.writer.closed)

    def test_opens_writes_closes_init_reads_closes(self):
        # open
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        wal.clear()

        # write and flush
        op = Write(WriterOps.SET, "a", 23)
        wal.append(op)
        wal.append(op)

        # close
        wal.close()
        self.assertTrue(wal.writer.closed)

        # re-init
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(wal.writer.closed)

        # read
        self.assertEquals(list(wal.read_all()), [op, op])

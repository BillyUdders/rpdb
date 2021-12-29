import unittest

from rpdb.operations import Write, WriterOps
from rpdb.wal import WriteAheadLog

WAL_FILE_LOCATION = "/tmp/wal.dat"


class WALTestCase(unittest.TestCase):
    def test_opens_writes_closes(self):
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(wal.writer.closed)
        wal.append(Write(WriterOps.SET, "a", "23"))
        wal.close()
        self.assertTrue(wal.writer.closed)

    def test_opens_writes_closes_init_reads_closes(self):
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        wal.clear()

        op = Write(WriterOps.SET, "a", "23")
        wal.append(op)
        wal.append(op)

        wal.close()
        self.assertTrue(wal.writer.closed)

        wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(wal.writer.closed)
        self.assertEqual(list(wal.read_all()), [op, op])

    def test_none_value(self):
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        wal.clear()
        op = Write(WriterOps.SET, "a", None)
        wal.append(op)
        wal.append(op)
        self.assertEqual(list(wal.read_all()), [op, op])

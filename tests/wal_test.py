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
        op2 = Write(WriterOps.SET, "a", "34")
        op3 = Write(WriterOps.SET, "a", "45")
        op4 = Write(WriterOps.UNSET, "a")
        wal.append(op)
        wal.append(op2)
        wal.append(op3)
        wal.append(op4)

        wal.close()
        self.assertTrue(wal.writer.closed)

        wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(wal.writer.closed)
        self.assertEqual([op, op2, op3, op4], list(wal.read()))

    def test_none_value(self):
        wal = WriteAheadLog(WAL_FILE_LOCATION)
        wal.clear()

        op = Write(WriterOps.SET, "a", None)
        # op2 = Write(WriterOps.SET, "a", "") Fails
        wal.append(op)

        self.assertEqual(list(wal.read()), [op])

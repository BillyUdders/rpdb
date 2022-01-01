import unittest

from rpdb.operations import Write, WriterOps
from rpdb.wal import WriteAheadLog

WAL_FILE_LOCATION = "/tmp/wal.dat"


class WALTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.wal = WriteAheadLog(WAL_FILE_LOCATION)

    def tearDown(self) -> None:
        self.wal.clear()

    def test_opens_writes_closes(self):
        self.assertFalse(self.wal.writer.closed)
        self.wal.append(Write(WriterOps.SET, "a", "23"))

        self.wal.close()
        self.assertTrue(self.wal.writer.closed)

    def test_opens_writes_closes_init_reads_closes(self):
        op = Write(WriterOps.SET, "a", "23")
        op2 = Write(WriterOps.SET, "a", "34")
        op3 = Write(WriterOps.SET, "a", "45")
        op4 = Write(WriterOps.UNSET, "a")
        self.wal.append(op)
        self.wal.append(op2)
        self.wal.append(op3)
        self.wal.append(op4)

        self.wal.close()
        self.assertTrue(self.wal.writer.closed)

        self.wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(self.wal.writer.closed)
        self.assertEqual([op, op2, op3, op4], list(self.wal))

    def test_none_value(self):
        op = Write(WriterOps.SET, "a", None)
        # op2 = Write(WriterOps.SET, "a", "") Fails
        self.wal.append(op)

        self.assertEqual(list(self.wal), [op])

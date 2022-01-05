import unittest

from rpdb.operations import Write, WriterOps
from rpdb.wal import WriteAheadLog

WAL_FILE_LOCATION = "/tmp/wal.dat"


class WALTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.wal = WriteAheadLog(WAL_FILE_LOCATION)

    def tearDown(self) -> None:
        self.wal.clear()

    def test_len(self):
        num_of_writes = 5
        for _ in range(num_of_writes):
            self.wal.append(Write(WriterOps.SET, "a", "23"))

        self.assertEqual(len(self.wal), num_of_writes)

    def test_contains(self):
        _a = Write(WriterOps.SET, "a", "23")
        _b = Write(WriterOps.SET, "b", "49")
        _c = Write(WriterOps.SET, "c", "FUCK")

        self.wal.append(_a)
        self.wal.append(_b)

        self.assertTrue(_a in self.wal)
        self.assertTrue(_b in self.wal)
        self.assertTrue(_c not in self.wal)
        self.assertTrue(23 not in self.wal)
        self.assertTrue("your mom" not in self.wal)

    def test_append_persist_reopen(self):
        # Open WAL, add some stuff
        op = Write(WriterOps.SET, "a", "23")
        op2 = Write(WriterOps.SET, "a", "34")
        op3 = Write(WriterOps.SET, "a", "45")
        op4 = Write(WriterOps.UNSET, "a")
        self.wal.append(op)
        self.wal.append(op2)
        self.wal.append(op3)
        self.wal.append(op4)

        # Close it
        self.wal.close()
        self.assertTrue(self.wal.writer.closed)

        # New WAL file, should be empty log
        self.wal = WriteAheadLog("/tmp/blah.dat")
        self.assertEqual([], list(self.wal))

        # Re-init the WAL with stuff in and read it back
        self.wal = WriteAheadLog(WAL_FILE_LOCATION)
        self.assertFalse(self.wal.writer.closed)
        self.assertEqual([op, op2, op3, op4], list(self.wal))

        # Add more stuff
        self.wal.append(op)
        self.wal.append(op2)
        self.wal.append(op3)
        self.wal.append(op4)

        # Should be in order
        self.assertEqual([op, op2, op3, op4, op, op2, op3, op4], list(self.wal))

    def test_none_value(self):
        op2 = Write(WriterOps.SET, "a", "")

        self.wal.append(op2)

        self.assertEqual(list(self.wal), [op2])

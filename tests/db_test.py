import unittest

from rpdb.db import DB
from rpdb.operations import Write, WriterOps


class DBTestCase(unittest.TestCase):
    def test_set_get_unset_exists(self):
        db = DB()

        db.set("a", "12")
        self.assertEqual(db.get("a"), "12")
        self.assertTrue(db.exists("a"))

        db.unset("a")
        self.assertRaises(KeyError, db.get, "a")
        self.assertFalse(db.exists("a"))

    def test_auto_commit_operations(self):
        db = DB()

        db.set("a", "12")

        wal = list(db.wal)
        self.assertEqual(
            wal,
            [
                Write(WriterOps.BEGIN, None),
                Write(WriterOps.SET, "a", "12"),
                Write(WriterOps.COMMIT, None),
            ],
        )

    def test_manual_transaction_operations(self):
        db = DB()

        with db.transaction():
            db.set("a", "12")
            db.set("b", "23")
            db.unset("b")
            operation_captor = list(db.wal)

        self.assertEqual(
            list(operation_captor),
            [
                Write(WriterOps.BEGIN, None),
                Write(WriterOps.SET, "a", 12),
                Write(WriterOps.SET, "b", 23),
                Write(WriterOps.UNSET, "b"),
                Write(WriterOps.COMMIT, None),
            ],
        )

    def test_tx_set_rollback(self):
        db = DB()

        db.set("a", "12")
        db.set("b", "23")
        self.assertEqual(db.get("a"), "12")
        self.assertEqual(db.get("b"), "23")

        with db.transaction():
            db.set("a", "49")
            db.unset("b")
            self.assertEqual(db.get("a"), "49")
            self.assertFalse(db.exists("b"))

        self.assertEqual(db.get("a"), "49")
        self.assertEqual(db.get("b"), "23")


if __name__ == "__main__":
    unittest.main()

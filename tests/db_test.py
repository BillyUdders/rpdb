import unittest

from rpdb.db import DB
from rpdb.operations import Write, WriterOps


class DBTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DB()

    def tearDown(self) -> None:
        self.db.wal.clear()
        self.db.storage.clear()

    def test_set_get_unset_exists(self):
        self.db.set("a", "12")
        self.assertEqual(self.db.get("a"), "12")
        self.assertTrue(self.db.exists("a"))

        self.db.unset("a")
        self.assertRaises(KeyError, self.db.get, "a")
        self.assertFalse(self.db.exists("a"))

    def test_auto_commit_operations(self):
        self.db.set("a", "12")

        self.assertEqual(
            self.__get_last_ops(),
            [
                Write(WriterOps.BEGIN, None),
                Write(WriterOps.SET, "a", "12"),
                Write(WriterOps.COMMIT, None),
            ],
        )

    def test_manual_transaction_operations(self):
        with self.db.transaction():
            self.db.set("a", "12")
            self.db.set("b", "23")
            self.db.unset("b")

        self.assertEqual(
            self.__get_last_ops(),
            [
                Write(WriterOps.BEGIN, None),
                Write(WriterOps.SET, "a", 12),
                Write(WriterOps.SET, "b", 23),
                Write(WriterOps.UNSET, "b"),
                Write(WriterOps.COMMIT, None),
            ],
        )

    def test_tx_set_rollback(self):
        self.db.set("a", "12")
        self.db.set("b", "23")
        self.assertEqual(self.db.get("a"), "12")
        self.assertEqual(self.db.get("b"), "23")

        with self.db.transaction():
            self.db.set("a", "49")
            self.db.unset("b")
            self.assertEqual(self.db.get("a"), "49")
            self.assertFalse(self.db.exists("b"))

        self.assertEqual(self.db.get("a"), "49")
        self.assertEqual(self.db.get("b"), "23")

    def __get_last_ops(self):
        return self.db.tx_log[-1].operations


if __name__ == "__main__":
    unittest.main()

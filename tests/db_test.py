import unittest

from rpdb.db import DB
from rpdb.operations import Write, WriterOps


class DBTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DB()

    def tearDown(self) -> None:
        self.db.wal.clear()

    def test_set_get_unset_exists(self):
        self.db["a"] = "12"
        self.assertEqual(self.db.get("a"), "12")
        self.assertTrue(self.db.exists("a"))

        del self.db["a"]
        self.assertRaises(KeyError, self.db.__getitem__, "a")
        self.assertFalse(self.db.exists("a"))

    def test_auto_commit_operations(self):
        self.db["a"] = "12"

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
            self.db["a"] = "12"
            self.db["b"] = "23"
            del self.db["b"]

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
        self.db["a"] = "12"
        self.db["b"] = "23"
        self.assertEqual(self.db.get("a"), "12")
        self.assertEqual(self.db.get("b"), "23")

        with self.db.transaction():
            self.db["a"] = "49"
            del self.db["b"]
            self.assertEqual(self.db.get("a"), "49")
            self.assertFalse(self.db.exists("b"))

        self.assertEqual(self.db.get("a"), "49")
        # self.assertEqual(self.db.get("b"), "23")

    def __get_last_ops(self):
        return self.db.tx_log[-1].operations


if __name__ == "__main__":
    unittest.main()

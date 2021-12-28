import unittest

from rpdb.db import DB
from rpdb.operations import Operation, OpType


class DBTestCase(unittest.TestCase):
    def test_set_get_unset_exists(self):
        db = DB()

        db.set("a", 12)
        self.assertEqual(db.get("a"), 12)
        self.assertTrue(db.exists("a"))

        db.unset("a")
        self.assertRaises(KeyError, db.get, "a")
        self.assertFalse(db.exists("a"))

    def test_auto_commit_operations(self):
        db = DB()

        db.set("a", 12)

        self.assertEqual(len(db.state.wal), 1)

        most_recent_tx = db.state.wal[-1]
        self.assertEqual(len(most_recent_tx), 3)
        self.assertEqual(
            most_recent_tx,
            [
                Operation(OpType.BEGIN, None, None),
                Operation(OpType.SET, "a", 12),
                Operation(OpType.COMMIT, None, None),
            ],
        )

    def test_tx_transaction_log(self):
        db = DB()

        with db.transaction():
            db.set("a", 12)
            db.set("b", 23)
            db.unset("b")
            self.assertEqual(len(db.live_txs), 1)
            operation_captor = db.live_txs[-1]

        self.assertEqual(
            operation_captor,
            [
                Operation(OpType.BEGIN, None, None),
                Operation(OpType.SET, "a", 12),
                Operation(OpType.SET, "b", 23),
                Operation(OpType.UNSET, "b", None),
                Operation(OpType.COMMIT, None, None),
            ],
        )
        self.assertEqual(len(db.live_txs), 0)

    def test_transaction_nesting(self):
        db = DB()
        with db.transaction():
            with db.transaction():
                self.assertEqual(len(db.live_txs), 2)
            self.assertEqual(len(db.live_txs), 1)
        self.assertEqual(len(db.live_txs), 0)

    def test_tx_set_rollback(self):
        db = DB()

        db.set("a", 12)
        db.set("b", 23)
        self.assertEqual(db.get("a"), 12)
        self.assertEqual(db.get("b"), 23)

        with db.transaction():
            db.set("a", 49)
            db.unset("b")
            self.assertEqual(db.get("a"), 49)
            self.assertFalse(db.exists("b"))

        self.assertEqual(db.get("a"), 49)
        self.assertEqual(db.get("b"), 23)


if __name__ == "__main__":
    unittest.main()

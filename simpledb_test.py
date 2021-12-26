import unittest

from main import Operation, OperationType, SimpleDB


class MyTestCase(unittest.TestCase):
    def test_set_get_unset_exists(self):
        db = SimpleDB()

        db.set("a", 12)
        self.assertEqual(db.get("a"), 12)
        self.assertTrue(db.exists("a"))

        db.unset("a")
        self.assertRaises(KeyError, db.get, "a")
        self.assertFalse(db.exists("a"))

    def test_auto_commit_operations(self):
        db = SimpleDB()

        db.set("a", 12)

        self.assertEqual(len(db.state.tx_log), 1)

        most_recent_tx = db.state.tx_log[-1]
        self.assertEqual(len(most_recent_tx), 3)
        self.assertEqual(
            most_recent_tx,
            [
                Operation(OperationType.BEGIN, None, None),
                Operation(OperationType.SET, "a", 12),
                Operation(OperationType.COMMIT, None, None),
            ],
        )

    def test_tx_transaction_log(self):
        db = SimpleDB()

        with db.transaction():
            db.set("a", 12)
            db.set("b", 23)
            db.unset("b")
            self.assertEqual(len(db.live_txs), 1)
            operation_captor = db.live_txs[-1]

        self.assertEqual(
            operation_captor,
            [
                Operation(OperationType.BEGIN, None, None),
                Operation(OperationType.SET, "a", 12),
                Operation(OperationType.SET, "b", 23),
                Operation(OperationType.UNSET, "b", None),
                Operation(OperationType.COMMIT, None, None),
            ],
        )
        self.assertEqual(len(db.live_txs), 0)

    def test_transaction_nesting(self):
        db = SimpleDB()
        with db.transaction():
            with db.transaction():
                self.assertEqual(len(db.live_txs), 2)
            self.assertEqual(len(db.live_txs), 1)
        self.assertEqual(len(db.live_txs), 0)

    def test_tx_set_rollback(self):
        db = SimpleDB()

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

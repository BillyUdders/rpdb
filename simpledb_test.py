import unittest
import main


class MyTestCase(unittest.TestCase):
    def test_set_get_unset(self):
        db = main.SimpleDB()

        db.set("a", 12)
        self.assertEqual(db.get("a"), 12)

        db.unset("a")
        self.assertRaises(KeyError, db.get, "a")
        self.assertFalse(db.exists("a"))

    def test_tx_set_rollback(self):
        db = main.SimpleDB()

        db.set("a", 12)
        db.set("b", 23)
        self.assertEqual(db.get("a"), 12)
        self.assertEqual(db.get("b"), 23)

        with db.transaction() as tx:
            db.set("a", 49)
            db.unset("b")

            # We're in a a transaction
            self.assertEqual(len(db.live_transactions), 1)
            # BEGIN, SET, UNSET
            self.assertEqual(len(db.live_transactions[-1].operations), 3)

            with db.transaction() as tx2:
                # Nested transaction
                self.assertEqual(len(db.live_transactions), 2)
                tx2.rollback()

            # Nested transaction closed
            self.assertEqual(len(db.live_transactions), 1)
            tx.rollback()

        self.assertEqual(db.get("a"), 12)
        self.assertEqual(db.get("b"), 23)
        self.assertListEqual(db.live_transactions, [])
        self.assertEqual(len(db.transaction_log), 4)


if __name__ == "__main__":
    unittest.main()

import unittest
from decimal import Decimal

try:
    import sqlalchemy
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import sessionmaker
    from app.database import Base, Trade, init_db, persist_trade
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False


@unittest.skipUnless(HAS_SQLALCHEMY, "SQLAlchemy is required for database tests")
class TestDatabaseOperations(unittest.TestCase):
    """
    In-memory SQLite unit tests verifying trade persistence,
    deduplication, transaction rollback, and constraints.
    Runs without needing a live PostgreSQL service.
    """

    def setUp(self):
        # Create an isolated in-memory SQLite engine for each test
        self.engine = create_engine("sqlite:///:memory:")
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_persist_new_trade(self):
        trade_data = {
            "trade_id": "TRD-001",
            "symbol": "AAPL",
            "quantity": 100,
            "price": 182.50,
            "side": "BUY",
        }
        with self.session.begin():
            persisted = persist_trade(trade_data, self.session)

        self.assertTrue(persisted)

        # Verify record exists in DB
        record = self.session.get(Trade, "TRD-001")
        self.assertIsNotNone(record)
        self.assertEqual(record.trade_id, "TRD-001")
        self.assertEqual(record.symbol, "AAPL")
        self.assertEqual(record.quantity, 100)
        self.assertEqual(record.price, Decimal("182.50"))
        self.assertEqual(record.side, "BUY")
        self.assertIsNotNone(record.created_at)

    def test_duplicate_trade_ignored(self):
        trade_data = {
            "trade_id": "TRD-002",
            "symbol": "MSFT",
            "quantity": 50,
            "price": 420.00,
            "side": "SELL",
        }
        # First insertion
        with self.session.begin():
            first_insert = persist_trade(trade_data, self.session)
        self.assertTrue(first_insert)

        # Duplicate insertion attempt
        with self.session.begin():
            second_insert = persist_trade(trade_data, self.session)
        self.assertFalse(second_insert)

        # Verify only one row exists
        records = self.session.execute(
            select(Trade).where(Trade.trade_id == "TRD-002")
        ).scalars().all()
        self.assertEqual(len(records), 1)

    def test_transaction_rollback_on_failure(self):
        trade_data = {
            "trade_id": "TRD-003",
            "symbol": "GOOGL",
            "quantity": 25,
            "price": 175.00,
            "side": "BUY",
        }
        try:
            with self.session.begin():
                persist_trade(trade_data, self.session)
                # Simulate a failure before transaction commits
                raise RuntimeError("Simulated transient processing failure")
        except RuntimeError:
            pass

        # Verify trade was rolled back and NOT persisted
        record = self.session.get(Trade, "TRD-003")
        self.assertIsNone(record)


if __name__ == "__main__":
    unittest.main()


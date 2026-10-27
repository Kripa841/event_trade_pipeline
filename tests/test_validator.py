import math
from decimal import Decimal
import unittest

from app.validator import validate_trade


class TestTradeValidator(unittest.TestCase):
    """
    Unit tests for the trade validation logic.
    Can be run via both pytest and python -m unittest.
    Runs without external dependencies (no Kafka or PostgreSQL required).
    """

    def setUp(self):
        self.valid_trade = {
            "trade_id": "TRD-1001",
            "symbol": "AAPL",
            "quantity": 100,
            "price": 182.50,
            "side": "BUY",
        }

    def test_valid_trade_success(self):
        result = validate_trade(self.valid_trade)
        self.assertEqual(result["trade_id"], "TRD-1001")
        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["quantity"], 100)
        self.assertEqual(result["price"], 182.50)
        self.assertEqual(result["side"], "BUY")

    def test_valid_trade_sell(self):
        trade = self.valid_trade.copy()
        trade["side"] = "SELL"
        result = validate_trade(trade)
        self.assertEqual(result["side"], "SELL")

    def test_valid_trade_with_decimal_price(self):
        trade = self.valid_trade.copy()
        trade["price"] = Decimal("199.95")
        result = validate_trade(trade)
        self.assertEqual(result["price"], 199.95)

    def test_valid_trade_with_integer_price(self):
        trade = self.valid_trade.copy()
        trade["price"] = 250
        result = validate_trade(trade)
        self.assertEqual(result["price"], 250.0)

    def test_non_dict_payload_raises(self):
        for invalid_payload in ["not-a-dict", None, [1, 2, 3], 12345]:
            with self.subTest(payload=invalid_payload):
                with self.assertRaises(ValueError):
                    validate_trade(invalid_payload)

    def test_missing_required_fields(self):
        for field in ["trade_id", "symbol", "quantity", "price", "side"]:
            with self.subTest(missing_field=field):
                trade = self.valid_trade.copy()
                del trade[field]
                with self.assertRaises(ValueError):
                    validate_trade(trade)

    def test_unexpected_fields_rejected(self):
        trade = self.valid_trade.copy()
        trade["unexpected_extra"] = "injected"
        with self.assertRaises(ValueError):
            validate_trade(trade)

    def test_invalid_trade_id(self):
        for invalid_id in ["", "   ", None, 123, []]:
            with self.subTest(trade_id=invalid_id):
                trade = self.valid_trade.copy()
                trade["trade_id"] = invalid_id
                with self.assertRaises(ValueError):
                    validate_trade(trade)

    def test_invalid_symbol(self):
        for invalid_symbol in ["", "   ", None, 456, {}]:
            with self.subTest(symbol=invalid_symbol):
                trade = self.valid_trade.copy()
                trade["symbol"] = invalid_symbol
                with self.assertRaises(ValueError):
                    validate_trade(trade)

    def test_invalid_quantity(self):
        for invalid_qty in [0, -1, -50, 10.5, "10", None, True, False, [10]]:
            with self.subTest(quantity=invalid_qty):
                trade = self.valid_trade.copy()
                trade["quantity"] = invalid_qty
                with self.assertRaises(ValueError):
                    validate_trade(trade)

    def test_invalid_price(self):
        for invalid_price in [
            0,
            -0.01,
            -150.0,
            float("nan"),
            float("inf"),
            float("-inf"),
            math.nan,
            math.inf,
            -math.inf,
            True,
            False,
            "150.00",
            None,
            [],
        ]:
            with self.subTest(price=invalid_price):
                trade = self.valid_trade.copy()
                trade["price"] = invalid_price
                with self.assertRaises(ValueError):
                    validate_trade(trade)

    def test_invalid_side(self):
        for invalid_side in ["HOLD", "buy", "sell", "BUY_ORDER", "", 1, None]:
            with self.subTest(side=invalid_side):
                trade = self.valid_trade.copy()
                trade["side"] = invalid_side
                with self.assertRaises(ValueError):
                    validate_trade(trade)


if __name__ == "__main__":
    unittest.main()

    def test_invalid_price(self):
        for invalid_price in [
            0,
            -0.01,
            -150.0,
            float("nan"),
            float("inf"),
            float("-inf"),
            math.nan,
            math.inf,
            -math.inf,
            True,
            False,
            "150.00",
            None,
            [],
        ]:
            with self.subTest(price=invalid_price):
                trade = self.valid_trade.copy()
                trade["price"] = invalid_price
                with self.assertRaises(ValueError):
                    validate_trade(trade)

    def test_invalid_side(self):
        for invalid_side in ["HOLD", "buy", "sell", "BUY_ORDER", "", 1, None]:
            with self.subTest(side=invalid_side):
                trade = self.valid_trade.copy()
                trade["side"] = invalid_side
                with self.assertRaises(ValueError):
                    validate_trade(trade)


if __name__ == "__main__":
    unittest.main()


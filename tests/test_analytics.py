import unittest
from app.analytics import RollingTradeWindow, detect_anomaly


class TestTradeAnalytics(unittest.TestCase):
    """
    Unit tests for rolling VWAP, OHLCV candlestick computations,
    and real-time volume/value anomaly detection.
    """

    def test_single_trade_vwap_and_ohlcv(self):
        window = RollingTradeWindow(symbol="AAPL")
        result = window.add_trade(price=100.0, quantity=10)

        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["latest_price"], 100.0)
        self.assertEqual(result["vwap"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["open"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["high"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["low"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["close"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["volume"], 10)

    def test_multi_trade_vwap_calculation(self):
        """
        Trade 1: 10 shares @ $100 -> Value = $1,000
        Trade 2: 20 shares @ $130 -> Value = $2,600
        Total Value = $3,600, Total Qty = 30
        Expected VWAP = 3600 / 30 = 120.0
        """
        window = RollingTradeWindow(symbol="AAPL")
        window.add_trade(price=100.0, quantity=10)
        result = window.add_trade(price=130.0, quantity=20)

        self.assertEqual(result["vwap"], 120.0)
        self.assertEqual(result["candlestick_ohlcv"]["open"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["high"], 130.0)
        self.assertEqual(result["candlestick_ohlcv"]["low"], 100.0)
        self.assertEqual(result["candlestick_ohlcv"]["close"], 130.0)
        self.assertEqual(result["candlestick_ohlcv"]["volume"], 30)

    def test_anomaly_detection_normal_trade(self):
        # A normal small trade should trigger NO alert
        alert = detect_anomaly(symbol="MSFT", price=100.0, quantity=10, trade_id="TRD-001")
        self.assertIsNone(alert)

    def test_anomaly_detection_volume_spike(self):
        # Quantity >= 80 triggers volume spike
        alert = detect_anomaly(symbol="AAPL", price=50.0, quantity=100, trade_id="TRD-BIG-QTY")
        self.assertIsNotNone(alert)
        self.assertEqual(alert["symbol"], "AAPL")
        self.assertTrue(any("Volume spike" in r for r in alert["alert_reasons"]))

    def test_anomaly_detection_value_spike(self):
        # Value >= $15,000 triggers high-value alert (50 * $400 = $20,000)
        alert = detect_anomaly(symbol="NVDA", price=400.0, quantity=50, trade_id="TRD-BIG-VAL")
        self.assertIsNotNone(alert)
        self.assertEqual(alert["symbol"], "NVDA")
        self.assertTrue(any("High-value transaction" in r for r in alert["alert_reasons"]))


if __name__ == "__main__":
    unittest.main()


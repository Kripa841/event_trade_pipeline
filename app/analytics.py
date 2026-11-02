import json
import logging
import os
import signal
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Deque, Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TradeAnalyticsEngine")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
TOPIC = os.getenv("KAFKA_TOPIC", "trade-events")
ANALYTICS_TOPIC = os.getenv("KAFKA_ANALYTICS_TOPIC", "trade-analytics")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "trade-alerts")
GROUP_ID = os.getenv("KAFKA_ANALYTICS_GROUP_ID", "trade-analytics-processors")

# Thresholds for real-time anomaly & risk detection
VOLUME_SPIKE_THRESHOLD = int(os.getenv("VOLUME_SPIKE_THRESHOLD", "80"))  # Single trade qty > 80 is a spike
VALUE_SPIKE_THRESHOLD = float(os.getenv("VALUE_SPIKE_THRESHOLD", "15000.0"))  # Total trade value > $15,000


class RollingTradeWindow:
    """
    Maintains a rolling window of recent trades for a given stock symbol.
    Calculates:
    1. VWAP (Volume-Weighted Average Price) = sum(Price * Qty) / sum(Qty)
    2. OHLCV (Open, High, Low, Close, Volume) candlestick bars
    3. Total volume and trade count
    """

    def __init__(self, symbol: str, max_window_size: int = 50):
        self.symbol = symbol
        self.trades: Deque[Dict[str, Any]] = deque(maxlen=max_window_size)
        self.total_volume: int = 0
        self.total_value: float = 0.0

    def add_trade(self, price: float, quantity: int, timestamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Record a trade and compute real-time VWAP and OHLCV.
        """
        trade_record = {
            "price": price,
            "quantity": quantity,
            "value": price * quantity,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        }
        self.trades.append(trade_record)

        # Recalculate rolling window aggregates
        prices = [t["price"] for t in self.trades]
        volumes = [t["quantity"] for t in self.trades]
        values = [t["value"] for t in self.trades]

        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]  # Latest price

        sum_volume = sum(volumes)
        sum_value = sum(values)

        # Formula: VWAP = Total Value / Total Volume
        vwap = (sum_value / sum_volume) if sum_volume > 0 else close_price

        return {
            "symbol": self.symbol,
            "latest_price": close_price,
            "vwap": round(vwap, 4),
            "candlestick_ohlcv": {
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": sum_volume,
            },
            "sample_size": len(self.trades),
            "calculated_at": datetime.now(timezone.utc).isoformat(),
        }


def detect_anomaly(symbol: str, price: float, quantity: int, trade_id: str) -> Optional[Dict[str, Any]]:
    """
    Detect real-time suspicious trades or sudden volume/value spikes.
    """
    trade_value = price * quantity
    reasons = []

    if quantity >= VOLUME_SPIKE_THRESHOLD:
        reasons.append(f"Volume spike detected: {quantity} shares exceeds threshold ({VOLUME_SPIKE_THRESHOLD})")

    if trade_value >= VALUE_SPIKE_THRESHOLD:
        reasons.append(f"High-value transaction: ${trade_value:,.2f} exceeds threshold (${VALUE_SPIKE_THRESHOLD:,.2f})")

    if reasons:
        return {
            "alert_id": f"ALT-{trade_id}",
            "symbol": symbol,
            "trade_id": trade_id,
            "quantity": quantity,
            "price": price,
            "trade_value": trade_value,
            "alert_reasons": reasons,
            "severity": "HIGH" if len(reasons) > 1 else "MEDIUM",
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
    return None


def main():
    logger.info("Starting Real-Time Trade Analytics & Anomaly Detection Engine...")
    logger.info("Kafka Broker: %s | Source Topic: %s", BOOTSTRAP_SERVERS, TOPIC)
    logger.info("Publishing Analytics to '%s' and Alerts to '%s'", ANALYTICS_TOPIC, ALERTS_TOPIC)

    try:
        from confluent_kafka import Consumer, KafkaError, Producer
    except ImportError:
        logger.critical("confluent-kafka package is required. Install via requirements.txt")
        sys.exit(1)

    # Kafka Consumer to read validated trades
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    # Kafka Producer to publish rolling VWAP analytics and real-time alerts
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "trade-analytics-publisher",
    })

    running = True

    def shutdown_handler(signum, frame):
        nonlocal running
        logger.info("Received termination signal. Stopping analytics engine gracefully...")
        running = False

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer.subscribe([TOPIC])
    windows: Dict[str, RollingTradeWindow] = defaultdict(lambda: RollingTradeWindow(symbol=""))

    logger.info("Analytics engine active. Awaiting trade stream...")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error encountered: %s", msg.error())
                continue

            raw_val = msg.value()
            if not raw_val:
                continue

            try:
                trade = json.loads(raw_val.decode("utf-8"))
            except Exception:
                # Malformed JSON is handled by the primary consumer's DLQ
                continue

            # Skip unvalidated items
            symbol = trade.get("symbol")
            price = trade.get("price")
            quantity = trade.get("quantity")
            trade_id = trade.get("trade_id", "UNKNOWN")

            if not symbol or not isinstance(price, (int, float)) or not isinstance(quantity, int):
                continue
            if price <= 0 or quantity <= 0:
                continue

            # 1. Update rolling window and calculate VWAP + OHLCV
            if symbol not in windows:
                windows[symbol] = RollingTradeWindow(symbol=symbol)

            analytics = windows[symbol].add_trade(price=float(price), quantity=quantity)

            logger.info(
                "📊 [ANALYTICS] %s | Price: $%.2f | VWAP: $%.4f | OHLC: [%.2f, %.2f, %.2f, %.2f] | Vol: %d",
                symbol,
                analytics["latest_price"],
                analytics["vwap"],
                analytics["candlestick_ohlcv"]["open"],
                analytics["candlestick_ohlcv"]["high"],
                analytics["candlestick_ohlcv"]["low"],
                analytics["candlestick_ohlcv"]["close"],
                analytics["candlestick_ohlcv"]["volume"],
            )

            # Publish analytics event to Kafka
            producer.produce(
                topic=ANALYTICS_TOPIC,
                key=symbol.encode("utf-8"),
                value=json.dumps(analytics).encode("utf-8"),
            )

            # 2. Check for real-time volume or high-value anomalies
            alert = detect_anomaly(symbol=symbol, price=float(price), quantity=quantity, trade_id=trade_id)
            if alert:
                logger.warning(
                    "🚨 [ALERT] Anomaly on %s (Trade %s)! %s",
                    symbol,
                    trade_id,
                    "; ".join(alert["alert_reasons"]),
                )
                producer.produce(
                    topic=ALERTS_TOPIC,
                    key=symbol.encode("utf-8"),
                    value=json.dumps(alert).encode("utf-8"),
                )

            producer.poll(0)

    finally:
        logger.info("Flushing analytics producer and closing consumer...")
        producer.flush(timeout=5.0)
        consumer.close()
        logger.info("Analytics engine closed cleanly.")


if __name__ == "__main__":
    main()

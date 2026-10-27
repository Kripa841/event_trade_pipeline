import json
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv

# Load local environment variables if available
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TradeProducer")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
DEFAULT_TOPIC = "trade-events"
TOPIC = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)


def make_delivery_callback(trade_id: str) -> Callable:
    """
    Produce delivery callback to acknowledge message delivery or report failure.
    """
    def delivery_report(err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("Failed to deliver trade '%s': %s", trade_id, err)
        else:
            logger.info(
                "Delivered trade '%s' to %s [partition %s] at offset %s",
                trade_id,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    return delivery_report


class TradeProducer:
    """
    Kafka Producer for streaming trade events.
    """

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or BOOTSTRAP_SERVERS
        logger.info("Connecting Kafka Producer to bootstrap servers: %s", self.bootstrap_servers)
        
        try:
            from confluent_kafka import Producer
        except ImportError as e:
            logger.error(
                "confluent-kafka is not installed in the current environment. "
                "Install it using: python -m pip install -r requirements.txt"
            )
            raise e

        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "trade-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 300,
        })

    def publish_trade(self, topic: str, trade_id: str, payload: Any) -> None:
        """
        Publish a trade event with trade_id as key.
        Accepts dicts, strings, or raw bytes.
        """
        if isinstance(payload, (dict, list)):
            val_bytes = json.dumps(payload).encode("utf-8")
        elif isinstance(payload, str):
            val_bytes = payload.encode("utf-8")
        elif isinstance(payload, bytes):
            val_bytes = payload
        else:
            val_bytes = str(payload).encode("utf-8")

        key_bytes = str(trade_id).encode("utf-8")

        try:
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=val_bytes,
                on_delivery=make_delivery_callback(trade_id),
            )
            # Serve delivery callback queue asynchronously
            self.producer.poll(0)
        except BufferError:
            logger.warning("Producer queue full. Polling and retrying...")
            self.producer.poll(1.0)
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=val_bytes,
                on_delivery=make_delivery_callback(trade_id),
            )

    def flush(self, timeout: float = 15.0) -> int:
        """
        Flush outstanding messages before exiting.
        Returns the number of messages still un-flushed.
        """
        logger.info("Flushing producer buffer (timeout: %.1fs)...", timeout)
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("%d message(s) still un-flushed after %.1fs", remaining, timeout)
        else:
            logger.info("All messages successfully delivered and flushed.")
        return remaining


def get_sample_trades() -> List[Dict[str, Any]]:
    """
    Return a demonstration set containing:
    1. Valid trades (should be validated and inserted into PostgreSQL)
    2. Duplicate trade (should be safely ignored by database / consumer)
    3. Invalid trades (should be rejected and routed to Dead Letter Queue)
    """
    return [
        # 1. Valid Trades
        {
            "trade_id": "TRD-1001",
            "symbol": "AAPL",
            "quantity": 100,
            "price": 182.50,
            "side": "BUY",
        },
        {
            "trade_id": "TRD-1002",
            "symbol": "MSFT",
            "quantity": 50,
            "price": 420.25,
            "side": "SELL",
        },
        {
            "trade_id": "TRD-1003",
            "symbol": "GOOGL",
            "quantity": 25,
            "price": 175.80,
            "side": "BUY",
        },
        # 2. Duplicate Trade (Re-publishing TRD-1001 to demonstrate deduplication)
        {
            "trade_id": "TRD-1001",
            "symbol": "AAPL",
            "quantity": 100,
            "price": 182.50,
            "side": "BUY",
        },
        # 3. Invalid Trades (to demonstrate validation & DLQ routing)
        {
            "trade_id": "TRD-INV-001",
            "symbol": "NVDA",
            "quantity": -10,  # Negative quantity
            "price": 120.00,
            "side": "BUY",
        },
        {
            "trade_id": "TRD-INV-002",
            "symbol": "TSLA",
            "quantity": 15,
            "price": -50.00,  # Negative price
            "side": "SELL",
        },
        {
            "trade_id": "TRD-INV-003",
            "symbol": "AMZN",
            "quantity": 10,
            "price": 180.00,
            "side": "HOLD",  # Invalid side
        },
        {
            "trade_id": "TRD-INV-004",
            "symbol": "",  # Empty symbol string
            "quantity": 10,
            "price": 50.00,
            "side": "BUY",
        },
    ]


def main():
    logger.info("Initializing trade event publisher to topic '%s'...", TOPIC)
    producer = TradeProducer()

    sample_trades = get_sample_trades()
    logger.info("Publishing %d sample trade events...", len(sample_trades))

    for trade in sample_trades:
        producer.publish_trade(
            topic=TOPIC,
            trade_id=trade["trade_id"],
            payload=trade,
        )

    # 4. Malformed message (Raw invalid JSON to verify DLQ handling)
    logger.info("Publishing 1 malformed non-JSON payload for DLQ verification...")
    producer.publish_trade(
        topic=TOPIC,
        trade_id="TRD-MALFORMED",
        payload="<<<MALFORMED_NON_JSON_PAYLOAD>>>",
    )

    remaining = producer.flush(timeout=15.0)
    if remaining > 0:
        logger.error("Failed to deliver %d message(s).", remaining)
        sys.exit(1)
    else:
        logger.info("Sample trades publishing finished successfully.")


if __name__ == "__main__":
    main()

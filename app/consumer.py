import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

from app.database import SessionLocal, init_db, persist_trade
from app.validator import validate_trade

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TradeConsumer")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
DEFAULT_TOPIC = "trade-events"
TOPIC = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
DEFAULT_DLQ_TOPIC = "trade-events-dlq"
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC)
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "trade-processors")


class DeadLetterQueueProducer:
    """
    Producer dedicated to publishing rejected or malformed events to a Dead Letter Topic.
    """

    def __init__(self, bootstrap_servers: str, dlq_topic: str):
        self.dlq_topic = dlq_topic
        try:
            from confluent_kafka import Producer
            self.producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "client.id": "trade-consumer-dlq",
                "acks": "all",
            })
            self._enabled = True
        except Exception as e:
            logger.warning("DLQ Producer initialization deferred/failed: %s", e)
            self.producer = None
            self._enabled = False

    def publish_to_dlq(
        self,
        raw_key: Optional[bytes],
        raw_value: Optional[bytes],
        reason: str,
        error_type: str,
        source_partition: int = -1,
        source_offset: int = -1,
    ) -> None:
        """
        Quarantine invalid or malformed message to the Dead Letter Topic.
        """
        key_str = raw_key.decode("utf-8", errors="replace") if raw_key else "UNKNOWN_KEY"
        val_str = raw_value.decode("utf-8", errors="replace") if raw_value else ""

        dlq_event = {
            "source_topic": TOPIC,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "key": key_str,
            "raw_payload": val_str,
            "rejection_reason": reason,
            "error_type": error_type,
            "rejected_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.warning(
            "Routing rejected event to DLQ topic '%s': key=%s, reason=%s",
            self.dlq_topic,
            key_str,
            reason,
        )

        if self._enabled and self.producer:
            try:
                self.producer.produce(
                    topic=self.dlq_topic,
                    key=key_str.encode("utf-8"),
                    value=json.dumps(dlq_event).encode("utf-8"),
                )
                self.producer.poll(0)
            except Exception as e:
                logger.error("Failed to publish to DLQ topic: %s", e)

    def flush(self, timeout: float = 5.0) -> None:
        if self._enabled and self.producer:
            self.producer.flush(timeout=timeout)


def main():
    logger.info("Initializing Trade Consumer...")
    logger.info("Kafka Bootstrap Servers: %s", BOOTSTRAP_SERVERS)
    logger.info("Subscribing to Topic: '%s', DLQ Topic: '%s', Consumer Group: '%s'", TOPIC, DLQ_TOPIC, GROUP_ID)

    # Initialize Database Schema
    try:
        init_db()
    except Exception as e:
        logger.error("Failed to connect/initialize database: %s", e)
        # Continue to start so container doesn't immediately crash if DB is starting up

    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError as e:
        logger.critical("confluent-kafka package is required. Install via requirements.txt.")
        sys.exit(1)

    dlq_producer = DeadLetterQueueProducer(BOOTSTRAP_SERVERS, DLQ_TOPIC)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Explicit manual offset commit after processing outcome
    })

    running = True

    def shutdown_handler(signum, frame):
        nonlocal running
        sig_name = signal.Signals(signum).name
        logger.info("Received shutdown signal (%s). Stopping gracefully...", sig_name)
        running = False

    # Handle termination signals cleanly
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer.subscribe([TOPIC])
    logger.info("Consumer active. Waiting for trade events from topic '%s'...", TOPIC)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition for %s [%s]", msg.topic(), msg.partition())
                else:
                    logger.error("Kafka error received: %s", msg.error())
                continue

            raw_key = msg.key()
            raw_value = msg.value()
            partition = msg.partition()
            offset = msg.offset()

            # 1. Deserialize message payload
            if raw_value is None:
                logger.warning("Received tombstone/null message at offset %d. Committing offset.", offset)
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                decoded_str = raw_value.decode("utf-8")
                trade_dict = json.loads(decoded_str)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error("Malformed message at offset %d: %s", offset, e)
                dlq_producer.publish_to_dlq(
                    raw_key=raw_key,
                    raw_value=raw_value,
                    reason=f"Malformed JSON / Decode Error: {e}",
                    error_type=type(e).__name__,
                    source_partition=partition,
                    source_offset=offset,
                )
                # Commit offset so bad message doesn't loop forever
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 2. Validate trade event
            try:
                validated_trade = validate_trade(trade_dict)
            except (ValueError, TypeError) as val_err:
                logger.warning("Trade validation failed at offset %d: %s", offset, val_err)
                dlq_producer.publish_to_dlq(
                    raw_key=raw_key,
                    raw_value=raw_value,
                    reason=f"Validation Error: {val_err}",
                    error_type=type(val_err).__name__,
                    source_partition=partition,
                    source_offset=offset,
                )
                # Commit offset after quarantine in DLQ
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 3. Persist valid trade to PostgreSQL with transaction handling
            db_success = False
            session = SessionLocal()
            try:
                with session.begin():
                    persisted = persist_trade(validated_trade, session)
                    if persisted:
                        logger.info(
                            "SUCCESS: Persisted trade '%s' [symbol=%s, qty=%d, price=%.2f, side=%s]",
                            validated_trade["trade_id"],
                            validated_trade["symbol"],
                            validated_trade["quantity"],
                            validated_trade["price"],
                            validated_trade["side"],
                        )
                    else:
                        logger.info("DUPLICATE: Trade '%s' was already recorded. Skipped safely.", validated_trade["trade_id"])
                db_success = True
            except (OperationalError, DBAPIError) as db_conn_err:
                logger.error(
                    "Database operational failure when saving trade '%s': %s. "
                    "Offset will NOT be committed and message will be retried.",
                    validated_trade.get("trade_id", "UNKNOWN"),
                    db_conn_err,
                )
                session.rollback()
                time.sleep(2.0)  # Backoff before retrying
                continue
            except SQLAlchemyError as db_err:
                logger.error("Database integrity/schema error for trade '%s': %s", validated_trade.get("trade_id"), db_err)
                session.rollback()
                # Quarantine to DLQ if unresolvable schema issue
                dlq_producer.publish_to_dlq(
                    raw_key=raw_key,
                    raw_value=raw_value,
                    reason=f"Database error: {db_err}",
                    error_type=type(db_err).__name__,
                    source_partition=partition,
                    source_offset=offset,
                )
                db_success = True  # Outcome handled via DLQ
            finally:
                session.close()

            # 4. Explicit offset commit only after successful outcome
            if db_success:
                try:
                    consumer.commit(message=msg, asynchronous=False)
                    logger.debug("Offset %d committed successfully for partition %d", offset, partition)
                except Exception as commit_err:
                    logger.error("Offset commit failed for offset %d: %s", offset, commit_err)

    finally:
        logger.info("Cleaning up resources...")
        dlq_producer.flush(timeout=5.0)
        consumer.close()
        logger.info("Consumer shutdown complete.")


if __name__ == "__main__":
    main()

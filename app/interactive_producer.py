import json
import logging
import os
import sys

from app.producer import TradeProducer, TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger("InteractiveProducer")


def prompt_trade():
    print("\n" + "="*50)
    print("📈 ENTER CUSTOM TRADE EVENT (or type 'exit' to quit)")
    print("="*50)

    trade_id = input("Trade ID (e.g., TRD-999): ").strip()
    if trade_id.lower() in ("exit", "quit", "q"):
        return None

    symbol = input("Stock Symbol (e.g., TSLA, NVDA, AAPL): ").strip().upper()
    raw_qty = input("Quantity / Shares (e.g., 25): ").strip()
    raw_price = input("Price USD (e.g., 240.50): ").strip()
    side = input("Side (BUY or SELL): ").strip().upper()

    # Try casting quantity and price
    try:
        qty = int(raw_qty)
    except ValueError:
        try:
            qty = float(raw_qty)
        except ValueError:
            qty = raw_qty  # Pass raw string to demonstrate validator rejection!

    try:
        price = float(raw_price)
    except ValueError:
        price = raw_price

    return {
        "trade_id": trade_id,
        "symbol": symbol,
        "quantity": qty,
        "price": price,
        "side": side,
    }


def main():
    print("Connecting to Kafka...")
    try:
        producer = TradeProducer()
    except Exception as e:
        logger.error("Failed to connect to Kafka: %s", e)
        sys.exit(1)

    print("Connected! You can now type your own trades and watch the browser dashboard update live!\n")

    while True:
        trade = prompt_trade()
        if trade is None:
            print("\nExiting interactive producer. Goodbye!")
            break

        print(f"\n🚀 Sending trade to Kafka topic '{TOPIC}': {trade}")
        producer.publish_trade(
            topic=TOPIC,
            trade_id=trade["trade_id"],
            payload=trade,
        )
        producer.flush(timeout=5.0)
        print("✅ Published! Check your browser at http://localhost:8000 to see it appear!\n")


if __name__ == "__main__":
    main()

import json
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from app.database import SessionLocal, Trade

logger = logging.getLogger("DashboardAPI")

app = FastAPI(title="Trade Event Pipeline - Live Mission Control")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "trade-events-dlq")


def read_dlq_events(max_messages: int = 50) -> List[Dict[str, Any]]:
    """
    Read quarantined events from the Kafka Dead-Letter Queue.
    """
    events = []
    try:
        from confluent_kafka import Consumer, KafkaError

        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "dashboard-dlq-reader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([DLQ_TOPIC])

        # Poll available DLQ messages
        for _ in range(max_messages):
            msg = consumer.poll(timeout=0.2)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                events.append(json.loads(msg.value().decode("utf-8")))
            except Exception:
                continue

        consumer.close()
    except Exception as e:
        logger.warning("DLQ poll deferred/unavailable: %s", e)

    # Return newest first
    return list(reversed(events))


@app.get("/", response_class=HTMLResponse)
def get_dashboard_html():
    """
    Serve the Live Mission Control web page.
    """
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/dashboard")
def get_dashboard_data():
    """
    JSON API providing live metrics:
    1. Valid trades from PostgreSQL
    2. Quarantined bad events from Kafka DLQ
    3. Computed rolling VWAP & volume summary per symbol
    """
    # 1. Fetch valid trades from PostgreSQL
    valid_trades = []
    try:
        session = SessionLocal()
        trades = session.execute(
            select(Trade).order_by(Trade.created_at.desc()).limit(100)
        ).scalars().all()
        valid_trades = [t.to_dict() for t in trades]
        session.close()
    except Exception as e:
        logger.error("Failed to query PostgreSQL: %s", e)

    # 2. Fetch quarantined events from DLQ
    dlq_trades = read_dlq_events()

    # 3. Compute VWAP map per symbol from valid trades
    vwap_map = {}
    symbol_groups = defaultdict(list)
    for t in valid_trades:
        symbol_groups[t["symbol"]].append(t)

    for sym, items in symbol_groups.items():
        total_vol = sum(i["quantity"] for i in items)
        total_val = sum(i["price"] * i["quantity"] for i in items)
        vwap = (total_val / total_vol) if total_vol > 0 else items[0]["price"]
        vwap_map[sym] = {
            "price": items[0]["price"],
            "vwap": vwap,
            "volume": total_vol,
        }

    return {
        "valid_trades": valid_trades,
        "dlq_trades": dlq_trades,
        "vwap_map": vwap_map,
    }


def main():
    import uvicorn
    uvicorn.run("app.dashboard:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()

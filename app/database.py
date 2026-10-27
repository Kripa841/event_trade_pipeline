import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    String,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

logger = logging.getLogger(__name__)

# Load local .env if present
load_dotenv()

DEFAULT_DATABASE_URL = "postgresql+psycopg://trader:traderpass@localhost:5432/tradesdb"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class Trade(Base):
    """
    SQLAlchemy model representing a validated trade event in PostgreSQL.
    """

    __tablename__ = "trades"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="chk_trades_quantity_positive"),
        CheckConstraint("price > 0", name="chk_trades_price_positive"),
        CheckConstraint("side IN ('BUY', 'SELL')", name="chk_trades_side_valid"),
    )

    trade_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "price": float(self.price),
            "side": self.side,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


def init_db(engine_override=None) -> None:
    """Initialize database schema if tables do not exist."""
    target_engine = engine_override or engine
    logger.info("Ensuring database schema exists...")
    Base.metadata.create_all(bind=target_engine)
    logger.info("Database schema initialized.")


def persist_trade(trade_data: Dict[str, Any], session: Session) -> bool:
    """
    Persist a validated trade dictionary into the database.
    Safely ignores duplicate trade_id entries.

    Args:
        trade_data: Validated dictionary containing trade details.
        session: Active SQLAlchemy Session.

    Returns:
        bool: True if a new record was added, False if duplicate was ignored.
    """
    trade_id = trade_data["trade_id"]
    existing = session.get(Trade, trade_id)
    if existing is not None:
        logger.warning("Duplicate trade detected and ignored: trade_id=%s", trade_id)
        return False

    trade_record = Trade(
        trade_id=trade_data["trade_id"],
        symbol=trade_data["symbol"],
        quantity=trade_data["quantity"],
        price=Decimal(str(trade_data["price"])),
        side=trade_data["side"],
    )
    session.add(trade_record)
    session.flush()
    logger.info("Persisted new trade record: trade_id=%s", trade_id)
    return True

def init_db(engine_override=None) -> None:
    """Initialize database schema if tables do not exist."""
    target_engine = engine_override or engine
    logger.info("Ensuring database schema exists...")
    Base.metadata.create_all(bind=target_engine)
    logger.info("Database schema initialized.")


def persist_trade(trade_data: Dict[str, Any], session: Session) -> bool:
    """
    Persist a validated trade dictionary into the database.
    Safely ignores duplicate trade_id entries.

    Args:
        trade_data: Validated dictionary containing trade details.
        session: Active SQLAlchemy Session.

    Returns:
        bool: True if a new record was added, False if duplicate was ignored.
    """
    trade_id = trade_data["trade_id"]
    existing = session.get(Trade, trade_id)
    if existing is not None:
        logger.warning("Duplicate trade detected and ignored: trade_id=%s", trade_id)
        return False

    trade_record = Trade(
        trade_id=trade_data["trade_id"],
        symbol=trade_data["symbol"],
        quantity=trade_data["quantity"],
        price=Decimal(str(trade_data["price"])),
        side=trade_data["side"],
    )
    session.add(trade_record)
    session.flush()
    logger.info("Persisted new trade record: trade_id=%s", trade_id)
    return True


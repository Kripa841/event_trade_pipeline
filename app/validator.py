import math
from decimal import Decimal
from typing import Any, Dict

REQUIRED_FIELDS = {
    "trade_id",
    "symbol",
    "quantity",
    "price",
    "side",
}

VALID_SIDES = {"BUY", "SELL"}


def validate_trade(trade: Any) -> Dict[str, Any]:
    """
    Validate a trade event dictionary.

    Requirements:
    - trade must be a JSON object (dict)
    - trade_id: non-empty string
    - symbol: non-empty string
    - quantity: positive integer (rejects booleans)
    - price: finite positive numeric value (rejects booleans, NaN, Inf)
    - side: 'BUY' or 'SELL'

    Raises:
        ValueError: If validation fails.
    Returns:
        Dict[str, Any]: Sanitized trade event dictionary.
    """
    if not isinstance(trade, dict):
        raise ValueError("Trade event must be a JSON object (dictionary)")

    missing = REQUIRED_FIELDS - trade.keys()
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")

    # Reject unexpected fields to enforce strict schema validation
    extra = set(trade.keys()) - REQUIRED_FIELDS
    if extra:
        raise ValueError(f"Unexpected fields in trade event: {sorted(extra)}")

    # Validate trade_id
    trade_id = trade["trade_id"]
    if not isinstance(trade_id, str) or not trade_id.strip():
        raise ValueError("trade_id must be a non-empty string")

    # Validate symbol
    symbol = trade["symbol"]
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")

    # Validate quantity
    quantity = trade["quantity"]
    # In Python, bool is a subclass of int, so explicitly check bool first
    if isinstance(quantity, bool) or not isinstance(quantity, int):
        raise ValueError("quantity must be an integer (booleans are not permitted)")
    if quantity <= 0:
        raise ValueError("quantity must be a positive integer (> 0)")

    # Validate price
    price = trade["price"]
    if isinstance(price, bool) or not isinstance(price, (int, float, Decimal)):
        raise ValueError("price must be a numeric value (int, float, or Decimal)")

    try:
        float_price = float(price)
    except (ValueError, OverflowError):
        raise ValueError("price cannot be converted to a valid float")

    if math.isnan(float_price) or math.isinf(float_price):
        raise ValueError("price must be a finite numeric value (NaN and Inf are not allowed)")

    if float_price <= 0:
        raise ValueError("price must be a positive numeric value (> 0)")

    # Validate side
    side = trade["side"]
    if not isinstance(side, str) or side not in VALID_SIDES:
        raise ValueError("side must be either 'BUY' or 'SELL'")

    return {
        "trade_id": trade_id.strip(),
        "symbol": symbol.strip().upper(),
        "quantity": quantity,
        "price": float_price,
        "side": side,
    }

    # Validate side
    side = trade["side"]
    if not isinstance(side, str) or side not in VALID_SIDES:
        raise ValueError("side must be either 'BUY' or 'SELL'")

    return {
        "trade_id": trade_id.strip(),
        "symbol": symbol.strip().upper(),
        "quantity": quantity,
        "price": float_price,
        "side": side,
    }


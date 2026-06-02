import uuid
import random
from datetime import datetime, timezone

ASSETS = ["BTC", "ETH", "AAPL", "GOOGL", "TSLA", "MSFT", "AMZN", "SPY", "QQQ", "GLD"]
SIDES = ["buy", "sell"]
EXCHANGES = ["NYSE", "NASDAQ", "BINANCE", "COINBASE", "LSE", "SGX"]

# Simulated mid prices that drift
_prices: dict[str, float] = {
    "BTC": 65000, "ETH": 3500, "AAPL": 185, "GOOGL": 175,
    "TSLA": 240, "MSFT": 420, "AMZN": 195, "SPY": 540,
    "QQQ": 470, "GLD": 235,
}


def _update_price(asset: str) -> float:
    mid = _prices[asset]
    change_pct = random.gauss(0, 0.002)  # 0.2% std per tick
    new_price = max(0.01, mid * (1 + change_pct))
    _prices[asset] = new_price
    return round(new_price, 8 if asset in ("BTC", "ETH") else 2)


def generate_trade() -> dict:
    asset = random.choice(ASSETS)
    price = _update_price(asset)
    qty = round(random.uniform(0.001, 10) if asset in ("BTC", "ETH") else random.uniform(1, 200), 4)
    return {
        "trade_id": str(uuid.uuid4()),
        "asset": asset,
        "side": random.choice(SIDES),
        "quantity": qty,
        "price": price,
        "total_value": round(qty * price, 4),
        "exchange": random.choice(EXCHANGES),
        "slippage_bps": round(random.expovariate(1/2), 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade_executed",
        "source": "financial",
    }


def generate_transfer() -> dict:
    amount = round(random.uniform(100, 50000), 2)
    return {
        "transfer_id": str(uuid.uuid4()),
        "from_account": str(uuid.uuid4())[:8],
        "to_account": str(uuid.uuid4())[:8],
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "GBP", "MYR", "SGD"]),
        "fee": round(amount * random.uniform(0.001, 0.003), 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "transfer",
        "source": "financial",
    }


def generate_event() -> dict:
    return generate_trade() if random.random() < 0.8 else generate_transfer()

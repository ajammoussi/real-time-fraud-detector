"""Real-time Binance WebSocket → Kafka producer.

Connects to the Binance public aggregate-trade WebSocket stream
(no API key required) for one or more trading pairs, maps each
trade message to the project fraud-detection schema, and publishes
it to the `transactions_raw` Kafka topic.

Binance WebSocket docs:
  https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
  Endpoint : wss://stream.binance.com:9443/stream?streams=<sym>@aggTrade/...
  Message  : {"stream":"btcusdt@aggTrade","data":{...}}

Field mapping  (Binance → ingestion schema):
  data.a  (aggregate tradeId)  → transaction_id   "txn_<symbol>_<a>"
  data.T  (trade time ms)      → timestamp        ISO-8601 UTC
  data.s  (symbol e.g.BTCUSDT) → currency         quote asset (USDT, BTC, …)
  data.p × data.q              → amount_usd       price × quantity (notional),
                                                  capped at 49 999.99 to stay
                                                  inside the lake pre-check bounds
  data.q  (quantity base ccy)  → tx_volume        float(q)
  data.m  (maker side)         → device_type      "exchange_maker"/"exchange_taker"

The topic record is deliberately UNLABELLED: a public trade feed carries no
fraud label, so `label` is absent here. Labels only ever enter the lake through
the synthetic seed path (scripts/ingestion_ctl.py seed), which is what the
classifier is actually trained on — see README "Dataset & labels".

`country` and `is_international` are derived from the quote asset, not from any
real geography, because a crypto feed has none. They stay in the payload for
schema compatibility, and `country` is explicitly *not* a model feature
(training/feature_engineering.py drops it).

Usage:
    python ingestion/producer.py                    # uses env-var symbols
    python ingestion/producer.py --symbols btcusdt,ethusdt
    python ingestion/producer.py --dry-run          # print without publishing
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import signal
import sys
from datetime import datetime, timezone

import typer
import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import get_settings

log = logging.getLogger("binance-producer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

_MERCHANT_CATS = [
    "crypto_exchange",
    "crypto_exchange",
    "crypto_exchange",  # weighted
    "gaming",
    "electronics",
    "travel",
    "finance",
]


# Quote assets Binance actually lists against; longest first so BTCFDUSD wins
# over USD. Anything else yields quote="UNKNOWN".
_KNOWN_QUOTES = tuple(
    sorted(
        (
            "USDT",
            "USDC",
            "BUSD",
            "FDUSD",
            "TUSD",
            "BTC",
            "ETH",
            "BNB",
            "EUR",
            "GBP",
            "TRY",
        ),
        key=len,
        reverse=True,
    )
)
_QUOTE_COUNTRY = {"USD": "US", "EUR": "DE", "GBP": "GB", "TRY": "TR"}
_FIAT_LIKE = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD"}


def _split_symbol(symbol: str) -> tuple[str, str]:
    """Split a Binance symbol into (base, quote), e.g. BTCUSDT → ("BTC", "USDT").

    Returns quote="UNKNOWN" when no known quote asset matches.
    """
    upper = symbol.upper()
    for quote in _KNOWN_QUOTES:
        if upper.endswith(quote) and len(upper) > len(quote):
            return upper[: -len(quote)], quote
    return upper, "UNKNOWN"


def _map_binance_trade(data: dict) -> dict:
    """Map a Binance aggTrade data payload to the fraud-detection schema.

    Args:
        data: The `data` sub-object from a Binance combined-stream message,
              i.e. the dict with keys e, E, a, s, p, q, f, l, T, m.

    Returns:
        A dict matching the fraud-detection transaction schema exactly.
    """
    symbol = data["s"]  # e.g. "BTCUSDT"
    trade_id = data["a"]  # integer aggregate trade id
    price = float(data["p"])
    qty = float(data["q"])
    trade_ts = datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc)
    is_maker = bool(data["m"])

    # Derive pseudo-user/merchant from trade_id and symbol deterministically
    user_bucket = (trade_id // 100) % 50_000
    merchant_bucket = (trade_id // 10) % 5_000
    cat_index = trade_id % len(_MERCHANT_CATS)
    ip_seed = f"{symbol}:{trade_id}".encode()

    # amount_usd = price × quantity, capped at 50 000 to stay inside GX bounds
    amount_usd = round(min(price * qty, 49_999.99), 2)
    _base, quote = _split_symbol(symbol)
    currency = quote
    country = _QUOTE_COUNTRY.get(quote, "ZZ")
    is_intl = quote not in _FIAT_LIKE

    return {
        "transaction_id": f"txn_{symbol.lower()}_{trade_id}",
        "timestamp": trade_ts.isoformat(),
        "user_id": f"usr_{user_bucket:05d}",
        "merchant_id": f"mrch_{merchant_bucket:04d}",
        "merchant_cat": _MERCHANT_CATS[cat_index],
        "amount_usd": amount_usd,
        "currency": currency,
        "country": country,
        "device_type": "exchange_maker" if is_maker else "exchange_taker",
        "ip_hash": hashlib.md5(ip_seed).hexdigest()[:8],
        "card_last4": f"{(trade_id % 9000) + 1000}",
        "is_international": is_intl,
        "hour_of_day": trade_ts.hour,
        "day_of_week": trade_ts.weekday(),
        # No `label` here: a public trade feed is unlabelled by definition.
        # extra provenance fields (stripped before model inference)
        "_source": "binance_ws",
        "_symbol": symbol,
        "_trade_id": trade_id,
        "_price": price,
        "_quantity": qty,
    }


def _build_ws_url(symbols: list[str]) -> str:
    """Build the Binance combined stream URL for aggTrade streams.

    Uses the public, no-auth combined stream endpoint:
      wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/...

    Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
          Section: "Combined Stream Events"
    """
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
    cfg = get_settings()
    return f"{cfg.binance_ws_base}?streams={streams}"


def _make_kafka_producer() -> KafkaProducer:
    cfg = get_settings()
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
    )


async def _stream(
    symbols: list[str], producer: KafkaProducer | None, dry_run: bool, max_messages: int
) -> None:
    cfg = get_settings()
    topic = cfg.kafka_topic_transactions_raw
    ws_url = _build_ws_url(symbols)
    count = 0

    log.info("Connecting to Binance WebSocket: %s", ws_url)
    log.info("Publishing to Kafka topic       : %s", topic)
    log.info("Subscribed symbols              : %s", symbols)

    async for websocket in websockets.connect(
        ws_url,
        ping_interval=20,
        ping_timeout=10,
        close_timeout=5,
    ):
        try:
            async for raw_msg in websocket:
                envelope = json.loads(raw_msg)
                # Combined stream wraps the payload: {"stream": "...", "data": {...}}
                data = envelope.get("data", envelope)
                if data.get("e") != "aggTrade":
                    continue  # skip non-trade messages (pings, errors)

                record = _map_binance_trade(data)
                key = record["transaction_id"]

                if dry_run:
                    print(json.dumps(record, indent=2))
                elif producer is not None:
                    try:
                        producer.send(topic, key=key, value=record)
                    except KafkaError as exc:
                        log.error("Kafka publish failed: %s", exc)

                count += 1
                if count % 100 == 0:
                    log.info("Published %d messages …", count)
                if max_messages and count >= max_messages:
                    log.info("Reached max_messages=%d, stopping.", max_messages)
                    return

        except websockets.ConnectionClosed as exc:
            log.warning("WebSocket closed (%s), reconnecting …", exc)
            await asyncio.sleep(2)
        except Exception as exc:
            log.error("Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(5)


app = typer.Typer(add_completion=False)


@app.command()
def main(
    symbols: str = typer.Option("", help="Comma-sep symbols, overrides env var"),
    dry_run: bool = typer.Option(False, help="Print records, do not publish to Kafka"),
    max_messages: int = typer.Option(0, help="Stop after N messages (0 = infinite)"),
):
    """Stream Binance trades → Kafka topic `transactions_raw`."""
    cfg = get_settings()
    sym_list = [s.strip().upper() for s in (symbols or cfg.binance_symbols).split(",") if s.strip()]

    producer = None if dry_run else _make_kafka_producer()

    def _shutdown(sig, _frame):
        log.info("Shutting down (signal %s) …", sig)
        if producer:
            producer.flush()
            producer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        asyncio.run(_stream(sym_list, producer, dry_run, max_messages))
    finally:
        if producer:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    app()

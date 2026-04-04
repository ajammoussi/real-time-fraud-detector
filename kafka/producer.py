"""Real-time Binance WebSocket → Kafka producer.

Connects to the Binance public aggregate-trade WebSocket stream
(no API key required) for one or more trading pairs, maps each
trade message to the project fraud-detection schema, and publishes
it to the `transactions_raw` Kafka topic.

Binance WebSocket docs:
  https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
  Endpoint : wss://stream.binance.com:9443/stream?streams=<sym>@aggTrade/...
  Message  : {"stream":"btcusdt@aggTrade","data":{...}}

Field mapping  (Binance → fraud schema):
  data.a  (aggregate tradeId)  → transaction_id   "txn_<a>"
  data.T  (trade time ms)      → timestamp        ISO-8601 UTC
  data.s  (symbol e.g.BTCUSDT) → currency_pair    (also derives currency/country)
  data.p  (price in quote ccy) → amount_usd       float(p)
  data.q  (quantity base ccy)  → tx_volume        float(q)
  data.m  (maker side)         → device_type      "exchange_maker"/"exchange_taker"

Fields without a direct Binance equivalent are given deterministic
derived values so the schema stays consistent with the rest of the
pipeline (GX suite, Feast, training).

Usage:
    python kafka/producer.py                        # uses env-var symbols
    python kafka/producer.py --symbols btcusdt,ethusdt
    python kafka/producer.py --dry-run              # print without publishing
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
    "crypto_exchange", "crypto_exchange", "crypto_exchange",  # weighted
    "gaming", "electronics", "travel", "finance",
]


def _extract_currencies_from_symbol(symbol: str) -> tuple[str, str]:
    """Extract base and quote currencies from Binance symbol.
    
    Binance symbols are typically formatted as BASEQUOTE (e.g., BTCUSDT, ETHBUSD).
    This function splits the symbol into base and quote currencies by recognizing
    common quote currencies.
    
    Args:
        symbol: Binance symbol (e.g., "BTCUSDT", "ETHBUSD", "XRPUSDC")
    
    Returns:
        Tuple of (base_currency, quote_currency)
    """
    # Common quote currencies in Binance
    quote_currencies = ["BUSD", "USDC", "USDT", "EUR", "GBP", "TRY", "RUB", "UAH", 
                        "BRL", "AUD", "CAD", "CHF", "CNY", "CZK", "DKK", "HKD", 
                        "HUF", "IDR", "ILS", "INR", "JPY", "KRW", "MXN", "MYR", 
                        "NOK", "NZD", "PHP", "PKR", "PLN", "SAR", "SEK", "SGD", 
                        "THB", "TWD", "ZAR", "BNB", "BTC", "ETH", "XRP", "ADA", 
                        "DOGE", "DOT", "UNI", "LINK", "LTC", "BCH", "XLM"]
    
    for quote in quote_currencies:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            if base:  # Ensure base is not empty
                return base, quote
    
    # Fallback: If no known quote found, assume last 3-4 chars are the quote
    if len(symbol) > 4:
        # assume quote is 3-4 characters
        for quote_len in [4, 3]:
            if len(symbol) > quote_len:
                potential_quote = symbol[-quote_len:]
                base = symbol[:-quote_len]
                if base and potential_quote.isalpha():
                    return base, potential_quote
    
    # Last resort: return the whole symbol as base and unknown as quote
    log.warning(f"Could not parse symbol '{symbol}', using default mapping")
    return symbol, "USD"


def _symbol_to_currency(symbol: str) -> str:
    """Get the quote currency from a Binance symbol."""
    _, quote = _extract_currencies_from_symbol(symbol)
    return quote


def _symbol_to_country(symbol: str) -> str:
    """Map a currency to a likely country based on the quote currency.
    
    Uses the quote currency to determine the most likely country.
    Returns 'US' as default for USD-based pairs or unknown currencies.
    """
    _, quote = _extract_currencies_from_symbol(symbol)
    
    # Map common fiat currencies to their primary countries
    currency_country_map = {
        "USD": "US", "EUR": "DE", "GBP": "GB", "JPY": "JP", "CNY": "CN",
        "KRW": "KR", "BRL": "BR", "CAD": "CA", "AUD": "AU", "CHF": "CH",
        "TRY": "TR", "RUB": "RU", "INR": "IN", "MXN": "MX", "ZAR": "ZA",
        "SGD": "SG", "HKD": "HK", "NZD": "NZ", "SEK": "SE", "NOK": "NO",
        "DKK": "DK", "PLN": "PL", "CZK": "CZ", "HUF": "HU", "ILS": "IL",
        "AED": "AE", "SAR": "SA", "THB": "TH", "VND": "VN", "IDR": "ID",
        "PHP": "PH", "PKR": "PK", "EGP": "EG", "NGN": "NG"
    }
    
    # For crypto quote currencies, use a deterministic mapping based on the currency hash
    if quote not in currency_country_map:
        # Crypto quotes: assign country based on deterministic hash
        hash_val = hashlib.md5(quote.encode()).hexdigest()
        # Common crypto-friendly countries
        crypto_countries = ["US", "SG", "JP", "KR", "AE", "CH", "DE", "GB"]
        index = int(hash_val[:8], 16) % len(crypto_countries)
        return crypto_countries[index]
    
    return currency_country_map.get(quote, "US")


def _map_binance_trade(data: dict) -> dict:
    """Map a Binance aggTrade data payload to the fraud-detection schema.

    Args:
        data: The `data` sub-object from a Binance combined-stream message,
              i.e. the dict with keys e, E, a, s, p, q, f, l, T, m.

    Returns:
        A dict matching the fraud-detection transaction schema exactly.
    """
    symbol   = data["s"]                             # e.g. "BTCUSDT"
    trade_id = data["a"]                             # integer aggregate trade id
    price    = float(data["p"])
    qty      = float(data["q"])
    trade_ts = datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc)
    is_maker = bool(data["m"])

    # Derive pseudo-user/merchant from trade_id and symbol deterministically
    user_bucket     = (trade_id // 100) % 50_000
    merchant_bucket = (trade_id // 10)  % 5_000
    cat_index       = (trade_id % len(_MERCHANT_CATS))
    ip_seed         = f"{symbol}:{trade_id}".encode()

    # amount_usd = price × quantity, capped at 50 000 to stay inside GX bounds
    amount_usd = round(min(price * qty, 49_999.99), 2)
    currency   = _symbol_to_currency(symbol)
    country    = _symbol_to_country(symbol)
    is_intl    = country != "US"

    return {
        "transaction_id":  f"txn_{symbol.lower()}_{trade_id}",
        "timestamp":       trade_ts.isoformat(),
        "user_id":         f"usr_{user_bucket:05d}",
        "merchant_id":     f"mrch_{merchant_bucket:04d}",
        "merchant_cat":    _MERCHANT_CATS[cat_index],
        "amount_usd":      amount_usd,
        "currency":        currency,
        "country":         country,
        "device_type":     "exchange_maker" if is_maker else "exchange_taker",
        "ip_hash":         hashlib.md5(ip_seed).hexdigest()[:8],
        "card_last4":      f"{(trade_id % 9000) + 1000}",
        "is_international": is_intl,
        "hour_of_day":     trade_ts.hour,
        "day_of_week":     trade_ts.weekday(),
        # label is always 0 here — real fraud labels come from downstream
        # labelling jobs or human review; we leave the field for schema compat
        "label":           0,
        # extra provenance fields (stripped before model inference)
        "_source":         "binance_ws",
        "_symbol":         symbol,
        "_trade_id":       trade_id,
        "_price":          price,
        "_quantity":       qty,
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


async def _stream(symbols: list[str], producer: KafkaProducer | None,
                  dry_run: bool, max_messages: int) -> None:
    cfg     = get_settings()
    topic   = cfg.kafka_topic_transactions_raw
    ws_url  = _build_ws_url(symbols)
    count   = 0

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
                key    = record["transaction_id"]

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
    symbols:      str  = typer.Option("", help="Comma-sep symbols, overrides env var"),
    dry_run:      bool = typer.Option(False, help="Print records, do not publish to Kafka"),
    max_messages: int  = typer.Option(0,  help="Stop after N messages (0 = infinite)"),
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

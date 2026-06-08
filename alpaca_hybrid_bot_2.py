#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import sys
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE LOGGING FUNCTIONS ---

def log_error_to_db(bot_name, error_msg):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO bot_errors (bot_name, error_message) VALUES (%s, %s)",
                            (bot_name, str(error_msg)))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to log error to DB: {e}")

def log_trade_to_db(bot_name, symbol, side, price, quantity, value, order_id, fee=0.0):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades (bot_name, exchange, symbol, side, price, quantity, value, fee, order_id, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (bot_name, 'Alpaca', symbol, side, float(price), float(quantity), float(value), float(fee), str(order_id)))
                conn.commit()
    except Exception as e:
        logger.error(f"Database write error: {e}")

def register_order_in_db(bot_name, order_id, symbol, side, price):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO bot_orders (order_id, bot_name, symbol, side, price, status)
                    VALUES (%s, %s, %s, %s, %s, 'OPEN')
                ''', (str(order_id), bot_name, symbol, side, float(price)))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to register order in DB: {e}")

def check_status(bot_name):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO bot_status (bot_name, last_update, status)
                    VALUES (%s, NOW(), 'RUNNING')
                    ON CONFLICT (bot_name) 
                    DO UPDATE SET last_update = NOW(), status = EXCLUDED.status;
                ''', (bot_name,))
                cur.execute("SELECT status FROM bot_status WHERE bot_name = %s", (bot_name,))
                row = cur.fetchone()
                if row and row[0] == 'STOP':
                    sys.exit(0)
                conn.commit()
    except Exception as e:
        logger.error(f"Heartbeat failed: {e}")

# --- BOT CLASS ---

class AlpacaTradingBot:
    def __init__(self):
        self.bot_name = os.getenv('BOT_NAME', 'alpaca_bot_2.py')  # your original name
        self.api_key = os.getenv("APCA_API_KEY_ID", "")
        self.secret_key = os.getenv("APCA_API_SECRET_KEY", "")
        if not os.getenv('DATABASE_URL'):
            raise ValueError("DATABASE_URL not set.")

        # Trading parameters
        self.symbol = "BTC/USD"          # Alpaca crypto uses slash format
        self.trade_size_usd = 10.0       # $10 per trade (paper trading)
        self.in_position = False
        self.entry_price = 0.0
        self.trailing_stop = 0.0
        self.cooldown_until = 0.0

        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client = CryptoHistoricalDataClient()
        check_status(self.bot_name)
        logger.info(f"Bot {self.bot_name} initialized. Trading {self.symbol}")

    def place_order_tracked(self, symbol, side, qty):
        """Submit market order and log it in the database."""
        order = self.trading_client.submit_order(
            order_data=MarketOrderRequest(symbol=symbol, qty=qty, side=side, time_in_force=TimeInForce.GTC)
        )
        register_order_in_db(self.bot_name, order.id, symbol, side.value, 0.0)
        return order

    async def sync_orders(self):
        """Check open orders and mark filled ones as CLOSED."""
        db_url = os.getenv('DATABASE_URL')
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT order_id, symbol FROM bot_orders WHERE bot_name = %s AND status = 'OPEN'",
                            (self.bot_name,))
                for oid, symbol in cur.fetchall():
                    try:
                        alpaca_order = self.trading_client.get_order_by_id(oid)
                        if alpaca_order.status == 'filled':
                            cur.execute("UPDATE bot_orders SET status = 'CLOSED' WHERE order_id = %s", (oid,))
                            log_trade_to_db(
                                self.bot_name, symbol, alpaca_order.side.value,
                                alpaca_order.filled_avg_price, alpaca_order.filled_qty,
                                float(alpaca_order.filled_avg_price) * float(alpaca_order.filled_qty),
                                oid, fee=0.0
                            )
                    except Exception as e:
                        logger.error(f"Error syncing order {oid}: {e}")
                conn.commit()

    async def get_latest_price_and_emas(self):
    """
    Fetch recent 15-minute bars, compute EMAs (9 & 21),
    return (current_price, fast_ema, slow_ema, prev_fast, prev_slow).
    Returns (None, ...) if insufficient data.
    """
    request_symbol = self.symbol  # keep slash

    end = datetime.now()
    start = end - timedelta(hours=12)
    request = CryptoBarsRequest(
        symbol_or_symbols=request_symbol,
        timeframe=TimeFrame.Minute,
        start=start,
        end=end,
        limit=200
    )
    bars = self.data_client.get_crypto_bars(request).data.get(request_symbol, [])
    if not bars or len(bars) < 22:
        logger.warning(f"Insufficient minute bars: {len(bars)}")
        return None, None, None, None, None

    df = pd.DataFrame([{'timestamp': b.timestamp, 'close': float(b.close)} for b in bars])
    df.sort_values('timestamp', inplace=True)
    df.set_index('timestamp', inplace=True)
    
    # FIX: use '15min' instead of '15T'
    ohlc_15 = df.resample('15min').agg({'close': 'last'}).dropna()
    closes = ohlc_15['close'].values

    if len(closes) < 22:
        logger.warning(f"Not enough 15-min bars: {len(closes)}")
        return None, None, None, None, None

    current_price = closes[-1]

    def ema(series, period):
        k = 2 / (period + 1)
        ema_val = series[0]
        for val in series[1:]:
            ema_val = val * k + ema_val * (1 - k)
        return ema_val

    fast_period = 9
    slow_period = 21
    fast_ema = ema(closes[-fast_period:], fast_period)
    slow_ema = ema(closes[-slow_period:], slow_period)

    prev_fast = ema(closes[-fast_period-1:-1], fast_period) if len(closes) > fast_period+1 else fast_ema
    prev_slow = ema(closes[-slow_period-1:-1], slow_period) if len(closes) > slow_period+1 else slow_ema

    return current_price, fast_ema, slow_ema, prev_fast, prev_slow

    async def run(self):
        logger.info("Starting Main Execution Loop...")
        while True:
            try:
                check_status(self.bot_name)
                await self.sync_orders()

                # Cooldown period after a sell
                if time.time() < self.cooldown_until:
                    logger.info("Cooldown active, skipping trading logic")
                    await asyncio.sleep(60)
                    continue

                # Get market data
                data = await self.get_latest_price_and_emas()
                if data[0] is None:
                    await asyncio.sleep(60)
                    continue

                current_price, fast_ema, slow_ema, prev_fast, prev_slow = data
                logger.info(f"Price: {current_price:.2f} | Fast EMA: {fast_ema:.2f} | Slow EMA: {slow_ema:.2f} | In position: {self.in_position}")

                # --- Trading logic ---
                if not self.in_position:
                    # Buy signal: fast EMA crosses above slow EMA, and price above slow EMA
                    if prev_fast <= prev_slow and fast_ema > slow_ema and current_price > slow_ema:
                        logger.info("*** MOMENTUM BUY SIGNAL ***")
                        qty = self.trade_size_usd / current_price
                        order = self.place_order_tracked(self.symbol, OrderSide.BUY, qty)
                        if order:
                            self.in_position = True
                            self.entry_price = current_price
                            self.trailing_stop = current_price * 0.97   # 3% trailing stop
                            logger.info(f"Bought {qty:.6f} {self.symbol} at ~{current_price:.2f}")

                else:
                    # Update trailing stop if price rises
                    if current_price > self.entry_price:
                        self.trailing_stop = max(self.trailing_stop, current_price * 0.97)

                    exit_signal = False
                    # Sell on bearish crossover
                    if fast_ema < slow_ema and prev_fast >= prev_slow:
                        logger.info("*** BEARISH CROSSOVER – SELL ***")
                        exit_signal = True
                    # Sell on trailing stop hit
                    elif current_price <= self.trailing_stop:
                        logger.info(f"*** TRAILING STOP HIT at {current_price:.2f} ***")
                        exit_signal = True

                    if exit_signal:
                        # Get current position quantity
                        try:
                            position = self.trading_client.get_position(self.symbol)
                            qty = float(position.qty)
                        except Exception:
                            qty = 0.0
                            logger.warning("No open position found, resetting state")
                            self.in_position = False
                            continue

                        if qty > 0:
                            order = self.place_order_tracked(self.symbol, OrderSide.SELL, qty)
                            if order:
                                self.in_position = False
                                self.cooldown_until = time.time() + 900   # 15 min cooldown
                                logger.info(f"Sold {qty:.6f} {self.symbol} – momentum exhausted")
                        else:
                            self.in_position = False

                await asyncio.sleep(60)

            except Exception as e:
                error_msg = f"Loop error: {str(e)}"
                logger.error(error_msg)
                log_error_to_db(self.bot_name, error_msg)
                await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        bot = AlpacaTradingBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.critical(f"FATAL CRASH: {str(e)}")
        sys.exit(1)

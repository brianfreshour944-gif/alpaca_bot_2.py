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
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


# --- DATABASE HELPERS ---

def log_error_to_db(bot_name, error_msg):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO bot_errors (bot_name, error_message) VALUES (%s, %s)",
                    (bot_name, str(error_msg)))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to log error to DB: {e}")

def log_trade_to_db(bot_name, symbol, side, price, quantity, value, order_id, fee=0.0):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades
                        (bot_name, exchange, symbol, side, price, quantity,
                         value, fee, order_id, timestamp)
                    VALUES (%s, 'Alpaca', %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (bot_name, symbol, side, float(price), float(quantity),
                      float(value), float(fee), str(order_id)))
                conn.commit()
    except Exception as e:
        logger.error(f"Database write error: {e}")

def register_order_in_db(bot_name, order_id, symbol, side, price):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_orders
                        (order_id, bot_name, symbol, side, price, status)
                    VALUES (%s, %s, %s, %s, %s, 'OPEN')
                    ON CONFLICT (order_id) DO NOTHING
                """, (str(order_id), bot_name, symbol, side, float(price)))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to register order in DB: {e}")

def save_position_state(bot_name, in_position, entry_price, trailing_stop):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE bot_status
                        ADD COLUMN IF NOT EXISTS in_position BOOLEAN DEFAULT FALSE""")
                cur.execute("""
                    ALTER TABLE bot_status
                        ADD COLUMN IF NOT EXISTS entry_price REAL DEFAULT 0""")
                cur.execute("""
                    ALTER TABLE bot_status
                        ADD COLUMN IF NOT EXISTS trailing_stop REAL DEFAULT 0""")
                cur.execute("""
                    INSERT INTO bot_status
                        (bot_name, in_position, entry_price, trailing_stop, last_update)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (bot_name) DO UPDATE
                        SET in_position   = EXCLUDED.in_position,
                            entry_price   = EXCLUDED.entry_price,
                            trailing_stop = EXCLUDED.trailing_stop,
                            last_update   = NOW()
                """, (bot_name, in_position, float(entry_price), float(trailing_stop)))
                conn.commit()
    except Exception as e:
        logger.error(f"save_position_state error: {e}")

def load_position_state(bot_name):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return False, 0.0, 0.0
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                # Ensure columns exist before querying — safe on existing DBs
                cur.execute("""
                    ALTER TABLE bot_status
                        ADD COLUMN IF NOT EXISTS in_position BOOLEAN DEFAULT FALSE""")
                cur.execute("""
                    ALTER TABLE bot_status
                        ADD COLUMN IF NOT EXISTS entry_price REAL DEFAULT 0""")
                cur.execute("""
                    ALTER TABLE bot_status
                        ADD COLUMN IF NOT EXISTS trailing_stop REAL DEFAULT 0""")
                conn.commit()
                cur.execute("""
                    SELECT in_position, entry_price, trailing_stop
                    FROM bot_status WHERE bot_name = %s
                """, (bot_name,))
                row = cur.fetchone()
                if row:
                    return bool(row[0]), float(row[1] or 0), float(row[2] or 0)
                return False, 0.0, 0.0
    except Exception as e:
        logger.error(f"load_position_state error: {e}")
        return False, 0.0, 0.0

def check_status(bot_name):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_status (bot_name, last_update, status)
                    VALUES (%s, NOW(), 'RUNNING')
                    ON CONFLICT (bot_name)
                    DO UPDATE SET last_update = NOW(),
                        status = CASE WHEN bot_status.status = 'STOP'
                                      THEN 'STOP' ELSE 'RUNNING' END
                """, (bot_name,))
                conn.commit()
                cur.execute(
                    "SELECT status FROM bot_status WHERE bot_name = %s", (bot_name,))
                row = cur.fetchone()
                if row and row[0] == 'STOP':
                    logger.info("🛑 STOP signal received. Exiting.")
                    sys.exit(0)
    except Exception as e:
        logger.error(f"Heartbeat failed: {e}")


# --- BOT CLASS ---

class AlpacaTradingBot:
    def __init__(self):
        self.bot_name  = os.getenv('BOT_NAME', 'alpaca_bot_2')
        self.api_key   = os.getenv('APCA_API_KEY_ID', '')
        self.secret_key = os.getenv('APCA_API_SECRET_KEY', '')

        if not os.getenv('DATABASE_URL'):
            raise ValueError("DATABASE_URL not set.")

        self.symbol         = "BTC/USD"
        self.trade_size_usd = float(os.getenv('TRADE_USD', '50.0'))
        self.cooldown_until = 0.0

        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client    = CryptoHistoricalDataClient()

        # Restore position state from DB
        self.in_position, self.entry_price, self.trailing_stop = \
            load_position_state(self.bot_name)

        if self.in_position:
            logger.info(
                f"♻️  Restored: in_position=True | entry={self.entry_price:.2f} | "
                f"stop={self.trailing_stop:.2f}")
            # Cross-check against Alpaca
            if self._get_position_qty() == 0.0:
                logger.warning("DB says in_position but no BTC on Alpaca — resetting.")
                self.in_position   = False
                self.entry_price   = 0.0
                self.trailing_stop = 0.0
                save_position_state(self.bot_name, False, 0.0, 0.0)
        else:
            logger.info("No saved position — starting fresh.")

        check_status(self.bot_name)
        logger.info(
            f"Bot {self.bot_name} initialized | symbol={self.symbol} | "
            f"trade_size=${self.trade_size_usd}")

    # ------------------------------------------------------------------
    # POSITION HELPERS
    # ------------------------------------------------------------------

    def _get_position_qty(self) -> float:
        """
        Try multiple symbol formats to find the BTC position.
        Returns 0.0 if none found.
        """
        for sym in ["BTCUSD", "BTC/USD", "BTC"]:
            try:
                pos = self.trading_client.get_position(sym)
                qty = float(pos.qty)
                if qty > 0:
                    return qty
            except Exception:
                continue
        # Fallback: scan all positions
        try:
            for p in self.trading_client.get_all_positions():
                if 'BTC' in p.symbol.upper():
                    return float(p.qty)
        except Exception as e:
            logger.error(f"get_all_positions failed: {e}")
        return 0.0

    def place_order_tracked(self, symbol, side, qty):
        try:
            order = self.trading_client.submit_order(
                order_data=MarketOrderRequest(
                    symbol=symbol, qty=qty,
                    side=side, time_in_force=TimeInForce.GTC)
            )
            register_order_in_db(self.bot_name, order.id, symbol, side.value, 0.0)
            return order
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    async def sync_orders(self):
        db_url = os.getenv('DATABASE_URL')
        if not db_url: return
        try:
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT order_id, symbol FROM bot_orders "
                        "WHERE bot_name = %s AND status = 'OPEN'",
                        (self.bot_name,))
                    for oid, symbol in cur.fetchall():
                        try:
                            ao = self.trading_client.get_order_by_id(oid)
                            if ao.status.value == 'filled':
                                cur.execute(
                                    "UPDATE bot_orders SET status = 'CLOSED' "
                                    "WHERE order_id = %s", (oid,))
                                avg  = float(ao.filled_avg_price or 0)
                                fqty = float(ao.filled_qty or 0)
                                log_trade_to_db(
                                    self.bot_name, symbol, ao.side.value,
                                    avg, fqty, avg * fqty, oid)
                                if ao.side == OrderSide.SELL:
                                    self.in_position   = False
                                    self.entry_price   = 0.0
                                    self.trailing_stop = 0.0
                                    save_position_state(self.bot_name, False, 0.0, 0.0)
                                    logger.info(
                                        f"✅ Sell confirmed filled @ {avg:.2f}")
                        except Exception as e:
                            logger.error(f"Error syncing order {oid}: {e}")
                    conn.commit()
        except Exception as e:
            logger.error(f"sync_orders error: {e}")

    # ------------------------------------------------------------------
    # INDICATORS — fixed EMA using full series
    # ------------------------------------------------------------------

    def _ema_series(self, values: list, period: int) -> list:
        """
        Compute EMA over the full series and return all values.
        Seeds with SMA of first `period` candles, then applies
        the exponential multiplier for the rest.
        """
        if len(values) < period:
            return [values[-1]] * len(values)
        k   = 2.0 / (period + 1)
        ema = [sum(values[:period]) / period]
        for v in values[period:]:
            ema.append(v * k + ema[-1] * (1 - k))
        pad = [ema[0]] * (len(values) - len(ema))
        return pad + ema

    async def get_latest_price_and_emas(self):
        """
        Fetch 24h of minute bars, resample to 15-min candles,
        compute 9 & 21 EMA over the full series.
        Returns (price, fast_ema, slow_ema, prev_fast, prev_slow)
        or (None, ...) on failure.
        """
        end   = datetime.now()
        start = end - timedelta(hours=24)
        request = CryptoBarsRequest(
            symbol_or_symbols=self.symbol,
            timeframe=TimeFrame.Minute,
            start=start, end=end, limit=1500)
        bars = self.data_client.get_crypto_bars(request).data.get(self.symbol, [])

        if len(bars) < 200:
            logger.warning(f"Insufficient minute bars: {len(bars)}")
            return None, None, None, None, None

        df = pd.DataFrame([
            {'timestamp': b.timestamp, 'close': float(b.close)} for b in bars
        ])
        df.sort_values('timestamp', inplace=True)
        df.set_index('timestamp', inplace=True)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        ohlc_15 = df.resample('15min').agg({'close': 'last'}).dropna()
        closes  = list(ohlc_15['close'].values)

        if len(closes) < 25:
            logger.warning(f"Not enough 15-min bars: {len(closes)}")
            return None, None, None, None, None

        # Compute EMA over FULL series — read last two values for crossover
        fast_series = self._ema_series(closes, 9)
        slow_series = self._ema_series(closes, 21)

        return (
            closes[-1],
            fast_series[-1],
            slow_series[-1],
            fast_series[-2],
            slow_series[-2],
        )

    # ------------------------------------------------------------------
    # MAIN LOOP
    # ------------------------------------------------------------------

    async def run(self):
        logger.info("Starting main execution loop...")
        while True:
            try:
                check_status(self.bot_name)
                await self.sync_orders()

                if time.time() < self.cooldown_until:
                    remaining = self.cooldown_until - time.time()
                    logger.info(f"⏳ Cooldown active — {remaining:.0f}s remaining")
                    await asyncio.sleep(60)
                    continue

                price, fast_ema, slow_ema, prev_fast, prev_slow = \
                    await self.get_latest_price_and_emas()

                if price is None:
                    await asyncio.sleep(60)
                    continue

                logger.info(
                    f"Price: {price:.2f} | Fast EMA: {fast_ema:.2f} | "
                    f"Slow EMA: {slow_ema:.2f} | "
                    f"Stop: {self.trailing_stop:.2f} | "
                    f"In position: {self.in_position}")

                # ------------------------------------------------------
                # ENTRY
                # ------------------------------------------------------
                if not self.in_position:
                    bullish_cross = prev_fast <= prev_slow and fast_ema > slow_ema
                    price_above   = price > slow_ema

                    if bullish_cross and price_above:
                        logger.info("*** MOMENTUM BUY SIGNAL ***")
                        qty   = self.trade_size_usd / price
                        order = self.place_order_tracked(
                            self.symbol, OrderSide.BUY, qty)
                        if order:
                            self.in_position   = True
                            self.entry_price   = price
                            self.trailing_stop = price * 0.97
                            save_position_state(
                                self.bot_name, True,
                                self.entry_price, self.trailing_stop)
                            logger.info(
                                f"✅ BUY {qty:.6f} BTC @ ~{price:.2f} | "
                                f"Stop: {self.trailing_stop:.2f}")
                    else:
                        logger.info(
                            f"No entry | cross={'yes' if bullish_cross else 'no'} "
                            f"price_above={'yes' if price_above else 'no'}")

                # ------------------------------------------------------
                # EXIT
                # ------------------------------------------------------
                else:
                    # Ratchet stop up as price rises
                    if price > self.entry_price:
                        new_stop = price * 0.97
                        if new_stop > self.trailing_stop:
                            self.trailing_stop = new_stop
                            save_position_state(
                                self.bot_name, True,
                                self.entry_price, self.trailing_stop)
                            logger.info(f"🔼 Stop raised to {self.trailing_stop:.2f}")

                    bearish_cross = fast_ema < slow_ema and prev_fast >= prev_slow
                    stop_hit      = price <= self.trailing_stop

                    if bearish_cross:
                        logger.info("*** BEARISH CROSSOVER — SELL ***")
                    elif stop_hit:
                        logger.info(
                            f"*** TRAILING STOP HIT — "
                            f"{price:.2f} <= {self.trailing_stop:.2f} ***")

                    if bearish_cross or stop_hit:
                        qty_to_sell = self._get_position_qty()
                        if qty_to_sell == 0.0:
                            logger.warning(
                                "Exit signal but no BTC found on Alpaca — "
                                "resetting position state.")
                            self.in_position   = False
                            self.entry_price   = 0.0
                            self.trailing_stop = 0.0
                            save_position_state(self.bot_name, False, 0.0, 0.0)
                        else:
                            order = self.place_order_tracked(
                                self.symbol, OrderSide.SELL, qty_to_sell)
                            if order:
                                pnl = (price - self.entry_price) * qty_to_sell
                                logger.info(
                                    f"✅ SELL {qty_to_sell:.6f} BTC @ ~{price:.2f} | "
                                    f"Est. PnL: ${pnl:.2f}")
                                self.in_position   = False
                                self.entry_price   = 0.0
                                self.trailing_stop = 0.0
                                self.cooldown_until = time.time() + 900
                                save_position_state(self.bot_name, False, 0.0, 0.0)

            except Exception as e:
                error_msg = f"Loop error: {e}"
                logger.error(error_msg)
                log_error_to_db(self.bot_name, error_msg)
                await asyncio.sleep(30)

            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        bot = AlpacaTradingBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.critical(f"FATAL CRASH: {e}")
        sys.exit(1)

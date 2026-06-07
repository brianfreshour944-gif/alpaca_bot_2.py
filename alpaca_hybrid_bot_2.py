#!/usr/bin/env python3
import asyncio
import logging
import os
import psycopg2
import sys
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import CryptoHistoricalDataClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE LOGGING ENGINE ---

def log_error_to_db(bot_name, error_msg):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO bot_errors (bot_name, error_message) VALUES (%s, %s)", (bot_name, str(error_msg)))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to log error to DB: {e}")

# UPDATED: Added 'fee' parameter
def log_trade_to_db(bot_name, symbol, side, price, quantity, value, order_id, fee=0.0):
    db_url = os.getenv('DATABASE_URL')
    if not db_url: return
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
    if not db_url: return
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
    if not db_url: return
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
        self.bot_name = os.getenv('BOT_NAME', 'Alpaca_Bot')
        self.api_key = os.getenv("APCA_API_KEY_ID", "")
        self.secret_key = os.getenv("APCA_API_SECRET_KEY", "")
        if not os.getenv('DATABASE_URL'):
            raise ValueError("DATABASE_URL not set.")
        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client = CryptoHistoricalDataClient()
        check_status(self.bot_name)

    def place_order_tracked(self, symbol, side, qty):
        order = self.trading_client.submit_order(
            order_data=MarketOrderRequest(symbol=symbol, qty=qty, side=side, time_in_force=TimeInForce.GTC)
        )
        register_order_in_db(self.bot_name, order.id, symbol, side.value, 0.0)
        return order

    async def sync_orders(self):
        db_url = os.getenv('DATABASE_URL')
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT order_id, symbol FROM bot_orders WHERE bot_name = %s AND status = 'OPEN'", (self.bot_name,))
                for (oid, symbol) in cur.fetchall():
                    alpaca_order = self.trading_client.get_order_by_id(oid)
                    if alpaca_order.status == 'filled':
                        cur.execute("UPDATE bot_orders SET status = 'CLOSED' WHERE order_id = %s", (oid,))
                        # Log with 0.0 fee (Alpaca API doesn't provide per-order fee in standard call)
                        log_trade_to_db(self.bot_name, symbol, alpaca_order.side.value, alpaca_order.filled_avg_price, alpaca_order.filled_qty, (float(alpaca_order.filled_avg_price) * float(alpaca_order.filled_qty)), oid, fee=0.0)
                conn.commit()

    async def run(self):
        logger.info("Starting Main Execution Loop...")
        while True:
            try:
                check_status(self.bot_name)
                await self.sync_orders()
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

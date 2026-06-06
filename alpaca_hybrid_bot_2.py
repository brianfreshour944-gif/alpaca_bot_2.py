#!/usr/bin/env python3
import asyncio
import pandas as pd
import numpy as np
import logging
import json
import os
import time
import psycopg2
import traceback
import sys
from datetime import datetime

from alpaca.trading.client import TradingClient
from alpaca.data.historical import CryptoHistoricalDataClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE LOGGING ENGINE ---
def log_trade_to_db(bot_name, symbol, side, price, quantity, value, order_id):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                # Matches your existing table: bot_name, exchange, symbol, side, price, quantity, value, order_id, timestamp
                cur.execute("""
                    INSERT INTO trades (bot_name, exchange, symbol, side, price, quantity, value, order_id, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (bot_name, 'Alpaca', symbol, side, float(price), float(quantity), float(value), str(order_id)))
                conn.commit()
    except Exception as e:
        logger.error(f"Database write error: {e}")

def log_bot_startup(bot_name):
    db_url = os.getenv('DATABASE_URL')
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades (bot_name, exchange, symbol, side, price, quantity, value, order_id, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (bot_name, 'Alpaca', 'N/A', 'SYSTEM', 0.0, 0.0, 0.0, 'STARTUP_SIGNAL'))
                conn.commit()
        logger.info(f"[{bot_name}] Heartbeat: Logged to DB.")
    except Exception as e:
        logger.error(f"Startup log failed: {e}")

# --- BOT CLASS ---
class AlpacaTradingBot:
    def __init__(self):
        self.bot_name = os.getenv('BOT_NAME', 'Alpaca_Bot')
        self.api_key = os.getenv("APCA_API_KEY_ID", "")
        self.secret_key = os.getenv("APCA_API_SECRET_KEY", "")
        
        if not os.getenv('DATABASE_URL'):
            raise ValueError("DATABASE_URL not set in environment.")
            
        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client = CryptoHistoricalDataClient()
        
        # Log Startup Heartbeat
        log_bot_startup(self.bot_name)
        
        logger.info(f"Bot {self.bot_name} initialized and DB verified.")

    async def run(self):
        logger.info("Starting Main Execution Loop...")
        while True:
            try:
                # ... (Keep your existing trading logic here)
                
                # USE THIS FOR TRADES:
                # log_trade_to_db(self.bot_name, symbol, 'BUY', price, qty, (price*qty), order_id)
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Loop error: {e}")
                await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        bot = AlpacaTradingBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.critical(f"FATAL CRASH: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)

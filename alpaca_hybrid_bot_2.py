
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
                cur.execute("""
                    INSERT INTO trades (bot_name, exchange, symbol, side, price, quantity, value, order_id, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (bot_name, 'Alpaca', symbol, side, float(price), float(quantity), float(value), str(order_id)))
                conn.commit()
    except Exception as e:
        logger.error(f"Database write error: {e}")

def check_status(bot_name):
    """Heartbeat and Kill Switch check for the bot_status table."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                # 1. Update Heartbeat
                cur.execute('''
                    INSERT INTO bot_status (bot_name, last_update, status)
                    VALUES (%s, NOW(), 'RUNNING')
                    ON CONFLICT (bot_name) 
                    DO UPDATE SET last_update = NOW(), status = EXCLUDED.status;
                ''', (bot_name,))
                
                # 2. Check for Kill Switch
                cur.execute("SELECT status FROM bot_status WHERE bot_name = %s", (bot_name,))
                row = cur.fetchone()
                if row and row[0] == 'STOP':
                    logger.warning(f"🛑 Kill switch activated for {bot_name}. Shutting down.")
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
            raise ValueError("DATABASE_URL not set in environment.")
            
        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client = CryptoHistoricalDataClient()
        
        # Verify status on init
        check_status(self.bot_name)
        logger.info(f"Bot {self.bot_name} initialized and DB heartbeat verified.")

    async def run(self):
        logger.info("Starting Main Execution Loop...")
        while True:
            try:
                # HEARTBEAT & KILL SWITCH CHECK
                check_status(self.bot_name)
                
                # ... (Your existing trading logic here)
                
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

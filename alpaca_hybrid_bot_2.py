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
from datetime import datetime, timedelta

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE LOGGING (DASHBOARD ENGINE) ---
def log_trade_to_db(symbol, side, price, qty=0.0, pnl=0.0, total_pnl=0.0, score=0.0, exit_reason=''):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades (bot_name, symbol, side, price, qty, pnl, total_pnl, score, exit_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (os.getenv('BOT_NAME', 'Alpaca_Bot'), symbol, side, price, qty, pnl, total_pnl, score, exit_reason))
                conn.commit()
    except Exception as e:
        logger.error(f"Database write error: {e}")

# --- BOT CLASS ---
class AlpacaTradingBot:
    def __init__(self):
        # Configuration
        self.api_key = os.getenv("APCA_API_KEY_ID", "")
        self.secret_key = os.getenv("APCA_API_SECRET_KEY", "")
        
        # Verify DB at startup
        if not os.getenv('DATABASE_URL'):
            raise ValueError("DATABASE_URL not set in environment.")
            
        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client = CryptoHistoricalDataClient()
        self.symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD']
        
        # ... (Keep your existing __init__ logic: score_calc, pnl, etc.)
        self.total_pnl = 0.0
        self.daily_pnl = 0.0
        logger.info("Bot initialized and DB verified.")

    async def run(self):
        logger.info("Starting Main Execution Loop...")
        while True:
            try:
                # ... (Your existing logic for fetching data and managing positions)
                
                # REPLACEMENT SNIPPETS:
                # Replace your old write_trade() calls with:
                # log_trade_to_db(symbol, 'BUY', fill_price, fill_qty, 0.0, self.total_pnl, score, 'ENTRY')
                # log_trade_to_db(symbol, 'SELL', fill_price, fill_qty, pnl_usd, self.total_pnl, score, exit_reason)
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Loop error: {e}")
                await asyncio.sleep(30)

# --- GLOBAL CRASH PROTECTION ---
if __name__ == "__main__":
    try:
        bot = AlpacaTradingBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.critical(f"FATAL CRASH: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)

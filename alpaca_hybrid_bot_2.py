#!/usr/bin/env python3
import asyncio
import pandas as pd
import numpy as np
import logging
import json
import os
import time
import psycopg2
from datetime import datetime, timedelta
# ... [Keep your imports: TradingClient, CryptoHistoricalDataClient, etc.]

# --- DATABASE LOGGING ---
def log_trade_to_db(symbol, side, price, qty=0.0, pnl=0.0, total_pnl=0.0, score=0.0, exit_reason=''):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trades (bot_name, symbol, side, price, qty, pnl, total_pnl, score, exit_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (os.getenv('BOT_NAME', 'Alpaca_Bot'), symbol, side, price, qty, pnl, total_pnl, score, exit_reason))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Database write error: {e}")

# ... [Keep your ScoreCalculator, calc_atr, and helper functions as they are]

class AlpacaTradingBot:
    def __init__(self):
        # ... [Keep your existing __init__ variables]
        # Remove init_csv()
        self.load_state()
        logger.info("Bot initialized with DB integration")

    # ... [Keep your existing fetch_data, get_positions_cache, submit_order]

    async def run(self):
        # ... [Inside your manage_position/buy section, replace write_trade with:]
        
        # When buying:
        log_trade_to_db(symbol, 'BUY', fill_price, fill_qty, 0.0, self.total_pnl, score, 'ENTRY')
        
        # When selling:
        log_trade_to_db(symbol, 'SELL', fill_price, fill_qty, pnl_usd, self.total_pnl, score, exit_reason)

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create the bot script directly
RUN cat > okx_hybrid_bot.py << 'EOF'
#!/usr/bin/env python3
# GROK_OKX_APEX_V8 - HYBRID STRATEGY WITH CSV EXPORT

import asyncio
import ccxt.pro as ccxtpro
import pandas as pd
import numpy as np
import logging
import json
import os
import time
import csv
from datetime import datetime

# Create /app directory if it doesn't exist (for volume mount)
os.makedirs('/app', exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

logger.info("Bot starting...")
logger.info(f"Current working directory: {os.getcwd()}")
logger.info(f"/app directory exists: {os.path.exists('/app')}")

def init_csv():
    csv_path = '/app/trades.csv'
    if not os.path.exists(csv_path):
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Symbol', 'Side', 'Price', 'PnL_USDT', 'Total_PnL_USDT', 'Score'])
        logger.info(f"Created CSV file at: {csv_path}")

def write_trade_to_csv(symbol, side, price, pnl_usdt=None, total_pnl=None, score=None):
    csv_path = '/app/trades.csv'
    try:
        with open(csv_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                symbol, side, price,
                pnl_usdt if pnl_usdt is not None else '',
                total_pnl if total_pnl is not None else '',
                score if score is not None else ''
            ])
        logger.info(f"CSV write successful: {side} {symbol} @ {price}")
    except Exception as e:
        logger.error(f"CSV write failed: {e}")

class HybridPredictor:
    def __init__(self):
        self.score_history = []
    # ... (include all methods from your original code) ...
    # For brevity, I'll skip the full class definition here.
    # Replace with your actual HybridPredictor class code.

class GrokApexIroncladBot:
    def __init__(self, paper_mode: bool = True, interval_minutes: int = 5):
        self.paper_mode = paper_mode
        self.interval_minutes = interval_minutes
        self.buy_threshold = 0.51
        self.sell_threshold = 0.49
        self.position_size = 0.01
        self.api_key = os.getenv("OKX_API_KEY", "")
        self.secret = os.getenv("OKX_SECRET_KEY", "")
        self.passphrase = os.getenv("OKX_PASSPHRASE", "")
        self.symbols = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
        self.ml = HybridPredictor()
        self.positions = {}
        self.running = True
        self.total_pnl = 0.0
        init_csv()
        logger.info("CSV logging initialized: /app/trades.csv")
        self.load_state()
    # ... rest of the bot class ...

if __name__ == "__main__":
    paper_mode = os.getenv('PAPER_MODE', 'true').lower() == 'true'
    bot = GrokApexIroncladBot(paper_mode=paper_mode, interval_minutes=5)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        bot.stop()
EOF

# Ensure the script is executable (optional)
RUN chmod +x okx_hybrid_bot.py

CMD ["python", "okx_hybrid_bot.py"]

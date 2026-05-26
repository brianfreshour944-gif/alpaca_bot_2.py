#!/usr/bin/env python3
# OKX HYBRID TRADING BOT

import asyncio
import ccxt.pro as ccxtpro
import pandas as pd
import numpy as np
import logging
import os
import csv
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# CSV LOGGING
# ==============================================================================

def write_trade(symbol, side, price, pnl_usdt=None, total_pnl=None, score=None):
    with open('trades.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            symbol, side, price,
            pnl_usdt if pnl_usdt is not None else '',
            total_pnl if total_pnl is not None else '',
            score if score is not None else ''
        ])

# Initialize CSV with headers
if not os.path.exists('trades.csv'):
    with open('trades.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Symbol', 'Side', 'Price', 'PnL_USDT', 'Total_PnL_USDT', 'Score'])

# ==============================================================================
# SCORE CALCULATOR
# ==============================================================================

class ScoreCalculator:
    def __init__(self):
        self.score_history = []
    
    def rsi(self, prices, period=14):
        if len(prices) < period + 1:
            return 50
        deltas = np.diff(prices[-period-1:])
        gain = np.mean(deltas[deltas > 0]) if any(deltas > 0) else 0.001
        loss = -np.mean(deltas[deltas < 0]) if any(deltas < 0) else 0.001
        return 100 - (100 / (1 + (gain / loss)))
    
    def ema(self, prices, period):
        alpha = 2 / (period + 1)
        ema = np.zeros_like(prices)
        ema[0] = prices[0]
        for i in range(1, len(prices)):
            ema[i] = prices[i] * alpha + ema[i-1] * (1 - alpha)
        return ema
    
    def compute(self, df):
        if df is None or len(df) < 50:
            return 0.5
        
        close = df['close'].values
        volume = df['volume'].values
        
        ma20 = np.mean(close[-20:])
        std20 = np.std(close[-20:])
        z_score = (close[-1] - ma20) / std20 if std20 > 0 else 0
        rsi_val = self.rsi(close)
        ema9 = self.ema(close, 9)
        ema21 = self.ema(close, 21)
        is_uptrend = ema9[-1] > ema21[-1] and close[-1] > ema9[-1]
        vol_avg = np.mean(volume[-10:]) if len(volume) >= 10 else 1
        vol_surge = volume[-1] / vol_avg if vol_avg > 0 else 1
        
        score = 0.5
        if z_score < -1.2:
            score += 0.35
        elif z_score < -0.8:
            score += 0.25
        elif z_score < -0.4:
            score += 0.15
        elif z_score > 1.2:
            score -= 0.35
        elif z_score > 0.8:
            score -= 0.25
        elif z_score > 0.4:
            score -= 0.15
        
        if rsi_val < 35:
            score += 0.10
        elif rsi_val < 45:
            score += 0.05
        elif rsi_val > 65:
            score -= 0.10
        elif rsi_val > 55:
            score -= 0.05
        
        if is_uptrend and score > 0.5:
            score += 0.08
        
        if vol_surge > 1.3:
            score += 0.05 if score > 0.5 else -0.05
        
        return max(0.0, min(1.0, score))

# ==============================================================================
# BOT
# ==============================================================================

class TradingBot:
    def __init__(self):
        self.buy_threshold = 0.51
        self.sell_threshold = 0.49
        self.position_size = 0.01
        self.symbols = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
        self.score_calc = ScoreCalculator()
        self.positions = {}
        self.total_pnl = 0.0
        
        self.api_key = os.getenv("OKX_API_KEY", "")
        self.secret = os.getenv("OKX_SECRET_KEY", "")
        self.passphrase = os.getenv("OKX_PASSPHRASE", "")
        
        logger.info("Bot initialized")
    
    async def fetch_data(self, exchange, symbol):
        try:
            ticker = await exchange.watch_ticker(symbol)
            price = ticker['last']
            ohlcv = await exchange.fetch_ohlcv(symbol, '5m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            return symbol, price, df
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return symbol, None, None
    
    async def run(self):
        exchange = ccxtpro.okx({
            'apiKey': self.api_key,
            'secret': self.secret,
            'password': self.passphrase,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        
        exchange.set_sandbox_mode(True)
        logger.info("=" * 50)
        logger.info("PAPER TRADING MODE - OKX HYBRID BOT")
        logger.info(f"Buy when score > {self.buy_threshold}")
        logger.info(f"Sell when score < {self.sell_threshold}")
        logger.info("=" * 50)
        
        await exchange.load_markets()
        
        while True:
            await asyncio.sleep(300)
            
            tasks = [self.fetch_data(exchange, s) for s in self.symbols]
            results = await asyncio.gather(*tasks)
            
            for symbol, price, df in results:
                if price is None or df is None:
                    continue
                
                score = self.score_calc.compute(df)
                logger.info(f"{symbol} | ${price:.2f} | Score: {score:.3f}")
                
                if score > self.buy_threshold and symbol not in self.positions:
                    logger.info(f"🟢 BUY: {symbol} @ ${price:.2f}")
                    write_trade(symbol, 'BUY', price, score=score)
                    self.positions[symbol] = {'price': price}
                
                elif score < self.sell_threshold and symbol in self.positions:
                    entry = self.positions[symbol]['price']
                    pnl = (price - entry) / entry * 10.0
                    self.total_pnl += pnl
                    logger.info(f"🔴 SELL: {symbol} @ ${price:.2f} | PnL: ${pnl:.2f} | Total: ${self.total_pnl:.2f}")
                    write_trade(symbol, 'SELL', price, pnl, self.total_pnl, score)
                    del self.positions[symbol]

if __name__ == "__main__":
    bot = TradingBot()
    asyncio.run(bot.run())

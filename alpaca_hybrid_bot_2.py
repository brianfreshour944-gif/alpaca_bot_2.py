#!/usr/bin/env python3
# ALPACA HYBRID TRADING BOT (Crypto Spot) — Optimized & Hardened Production Edition

import asyncio
import pandas as pd
import numpy as np
import logging
import json
import os
import csv
import time
from datetime import datetime, timedelta

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CSV LOGGING
# ==============================================================================
def init_csv():
    if not os.path.exists('trades.csv'):
        with open('trades.csv', 'w', newline='') as f:
            csv.writer(f).writerow([
                'Timestamp', 'Symbol', 'Side', 'Price', 'Qty',
                'PnL_USD', 'Total_PnL_USD', 'Score', 'ExitReason',
                'StopPrice', 'TargetPrice'
            ])

def write_trade(symbol, side, price, qty=None, pnl_usd=None, total_pnl=None,
                score=None, exit_reason=None, stop_price=None, target_price=None):
    with open('trades.csv', 'a', newline='') as f:
        csv.writer(f).writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            symbol, side, price,
            qty          if qty          is not None else '',
            f'{pnl_usd:.4f}'   if pnl_usd      is not None else '',
            f'{total_pnl:.4f}' if total_pnl    is not None else '',
            f'{score:.4f}'     if score         is not None else '',
            exit_reason  if exit_reason  is not None else '',
            f'{stop_price:.4f}'   if stop_price   is not None else '',
            f'{target_price:.4f}' if target_price is not None else '',
        ])

# ==============================================================================
# SCORE CALCULATOR
# ==============================================================================
class ScoreCalculator:

    def rsi(self, prices, period=14):
        if len(prices) < period + 1:
            return 50
        deltas = np.diff(prices[-period - 1:])
        gain = np.mean(deltas[deltas > 0]) if any(deltas > 0) else 0.001
        loss = -np.mean(deltas[deltas < 0]) if any(deltas < 0) else 0.001
        return 100 - (100 / (1 + (gain / loss)))

    def ema(self, prices, period):
        alpha = 2 / (period + 1)
        ema = np.zeros_like(prices, dtype=float)
        ema[0] = prices[0]
        for i in range(1, len(prices)):
            ema[i] = prices[i] * alpha + ema[i - 1] * (1 - alpha)
        return ema

    def compute(self, df):
        if df is None or len(df) < 50:
            return 0.5

        close  = df['close'].values.astype(float)
        volume = df['volume'].values.astype(float)

        ma20  = np.mean(close[-20:])
        std20 = np.std(close[-20:])
        z_score = (close[-1] - ma20) / std20 if std20 > 0 else 0

        rsi_val  = self.rsi(close)
        ema9     = self.ema(close, 9)
        ema21    = self.ema(close, 21)
        is_uptrend = ema9[-1] > ema21[-1] and close[-1] > ema9[-1]

        vol_avg   = np.mean(volume[-10:]) if len(volume) >= 10 else 1
        vol_surge = volume[-1] / vol_avg if vol_avg > 0 else 1

        score = 0.5

        if is_uptrend:
            if z_score < -1.2:   score += 0.35
            elif z_score < -0.8: score += 0.25
            elif z_score < -0.4: score += 0.15
            elif z_score > 1.2:  score -= 0.25  
            elif z_score > 0.8:  score -= 0.15
            elif z_score > 0.4:  score -= 0.08
            score += 0.08
        else:
            if z_score < -1.5:   score += 0.25
            elif z_score < -1.0: score += 0.10
            elif z_score > 0.8:  score -= 0.30  
            elif z_score > 0.4:  score -= 0.20

        if rsi_val < 35:   score += 0.10
        elif rsi_val < 45: score += 0.05
        elif rsi_val > 65: score -= 0.10
        elif rsi_val > 55: score -= 0.05

        if vol_surge > 1.3:
            score += 0.05 if score > 0.5 else -0.05

        return max(0.0, min(1.0, score))

def calc_atr(df, period=14):
    if df is None or len(df) < period + 1:
        return df['close'].iloc[-1] * 0.01

    if 'high' in df.columns and 'low' in df.columns:
        high  = df['high'].values.astype(float)
        low   = df['low'].values.astype(float)
        close = df['close'].values.astype(float)
        tr = np.maximum(
            high[1:] - low[1:],
            np.maximum(
                np.abs(high[1:] - close[:-1]),
                np.abs(low[1:]  - close[:-1])
            )
        )
        return float(np.mean(tr[-period:]))
    else:
        close = df['close'].values.astype(float)
        return float(np.mean(np.abs(np.diff(close[-(period + 1):]))))

# ==============================================================================
# MAIN BOT ENGINE
# ==============================================================================
class AlpacaTradingBot:

    def __init__(self):
        self.buy_threshold     = 0.65  
        self.sell_threshold    = 0.35  
        self.min_hold_bars     = 24    # Hold 24 hours minimum to survive standard noise
        self.sell_confirm_bars = 1
        self.position_size_usd = 100.0  
        self.atr_stop_mult      = 2.0   
        self.atr_target_mult    = 5.0   
        self.daily_loss_limit   = -150.0 
        self.max_daily_trades   = 10
        self.cycle_secs         = 3600  # Hourly loop interval

        # Symbols listed in standardized format
        self.symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD']

        self.api_key    = os.getenv("APCA_API_KEY_ID", "")
        self.secret_key = os.getenv("APCA_API_SECRET_KEY", "")

        if not self.api_key or not self.secret_key:
            logger.warning("Alpaca API keys missing — set APCA_API_KEY_ID and APCA_API_SECRET_KEY.")

        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)
        self.data_client    = CryptoHistoricalDataClient()

        self.score_calc = ScoreCalculator()

        self.positions         = {}
        self.cooldowns         = {}
        self.bearish_count     = {}
        self.total_pnl          = 0.0
        self.daily_pnl          = 0.0
        self.daily_trade_count = 0
        self.current_day       = datetime.now().date()

        init_csv()
        self.load_state()
        logger.info("Bot initialised (paper trading, crypto spot)")

    def save_state(self):
        cooldowns_s = {s: dt.isoformat() for s, dt in self.cooldowns.items()}
        with open('alpaca_state.json', 'w') as f:
            json.dump({
                'positions':         self.positions,
                'cooldowns':         cooldowns_s,
                'total_pnl':         self.total_pnl,
                'daily_pnl':         self.daily_pnl,
                'daily_trade_count': self.daily_trade_count,
                'current_day':       self.current_day.isoformat(),
            }, f)

    def load_state(self):
        if not os.path.exists('alpaca_state.json'):
            return
        try:
            with open('alpaca_state.json') as f:
                data = json.load(f)
            self.positions         = data.get('positions', {})
            self.total_pnl          = data.get('total_pnl', 0.0)
            self.daily_pnl          = data.get('daily_pnl', 0.0)
            self.daily_trade_count = data.get('daily_trade_count', 0)
            self.current_day       = datetime.fromisoformat(data['current_day']).date()
            now = datetime.now()
            self.cooldowns = {
                s: datetime.fromisoformat(v)
                for s, v in data.get('cooldowns', {}).items()
                if datetime.fromisoformat(v) > now
            }
            logger.info(f"State loaded — Trades today: {self.daily_trade_count} | Daily P&L: ${self.daily_pnl:.2f}")
        except Exception as e:
            logger.error(f"State load failed: {e}")

    def _norm(self, symbol):
        return symbol.replace('/', '').replace('-', '')

    async def get_positions_cache(self):
        try:
            loop = asyncio.get_running_loop()
            positions = await loop.run_in_executor(None, self.trading_client.get_all_positions)
            # Normalizing keys returned from Alpaca to ensure strict string matches
            return {p.symbol.replace('/', ''): {'qty': float(p.qty), 'avg_price': float(p.avg_entry_price)} for p in positions}
        except Exception as e:
            logger.error(f"Position cache error: {e}")
            return {}

    async def fetch_data(self, symbol):
        try:
            loop = asyncio.get_running_loop()
            req  = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame(1, TimeFrameUnit.Hour),
                limit=100,
            )
            bars = await loop.run_in_executor(None, self.data_client.get_crypto_bars, req)
            if symbol not in bars.data:
                return None, None
            rows = bars.data[symbol]
            df = pd.DataFrame({
                'open':   [b.open   for b in rows],
                'high':   [b.high   for b in rows],
                'low':    [b.low    for b in rows],
                'close':  [b.close  for b in rows],
                'volume': [b.volume for b in rows],
            })
            return rows[-1].close, df
        except Exception as e:
            logger.error(f"Data fetch error {symbol}: {e}")
            return None, None

    async def submit_order(self, symbol, side, usd_amount=None):
        try:
            loop = asyncio.get_running_loop()
            norm = self._norm(symbol)

            pre_price, _ = await self.fetch_data(symbol)
            if pre_price is None:
                return False, 0, 0

            if side == 'buy':
                qty = round((usd_amount / pre_price) - 0.0000005, 6)
                if qty <= 0:
                    return False, 0, 0
                order_req = MarketOrderRequest(
                    symbol=norm, qty=qty,
                    side=OrderSide.BUY, time_in_force=TimeInForce.GTC
                )
            else:
                positions = await self.get_positions_cache()
                if norm not in positions:
                    logger.error(f"No active execution track found to close: {symbol}")
                    return False, 0, 0
                qty = round(positions[norm]['qty'] - 0.0000005, 6)
                if qty <= 0:
                    qty = positions[norm]['qty']
                order_req = MarketOrderRequest(
                    symbol=norm, qty=qty,
                    side=OrderSide.SELL, time_in_force=TimeInForce.GTC
                )

            order = await loop.run_in_executor(None, self.trading_client.submit_order, order_req)
            
            # HARDENED FILL DETECTOR: Poll up to 5 seconds to grab actual execution data
            fill_price = pre_price
            for _ in range(5):
                try:
                    live_order = await loop.run_in_executor(None, self.trading_client.get_order_by_id, order.id)
                    if live_order.status == OrderStatus.FILLED and live_order.filled_avg_price is not None:
                        fill_price = float(live_order.filled_avg_price)
                        qty = float(live_order.filled_qty) if live_order.filled_qty else qty
                        break
                except Exception:
                    pass
                await asyncio.sleep(1)

            logger.info(f"ORDER {side.upper()} {qty} {symbol} Executed @ ${fill_price:.4f}")
            return True, qty, fill_price

        except Exception as e:
            logger.error(f"Order failed {symbol} {side}: {e}")
            return False, 0, 0

    async def run(self):
        logger.info("=" * 60)
        logger.info("PRODUCTION — ALPACA HYBRID BOT (Fixed / Hourly Engine)")
        logger.info("=" * 60)

        last_heartbeat = 0

        while True:
            try:
                now_t = time.time()
                if now_t - last_heartbeat >= 60:
                    logger.info(
                        f"[Heartbeat] Open positions: {len(self.positions)} | "
                        f"Daily P&L: ${self.daily_pnl:.2f} | Total P&L: ${self.total_pnl:.2f}"
                    )
                    last_heartbeat = now_t

                today = datetime.now().date()
                if today != self.current_day:
                    logger.info("New trading day — resetting daily counters.")
                    self.daily_pnl          = 0.0
                    self.daily_trade_count = 0
                    self.current_day       = today

                if self.daily_pnl <= self.daily_loss_limit:
                    logger.error(f"Daily loss limit hit (${self.daily_pnl:.2f}) — pausing execution.")
                    await asyncio.sleep(60)
                    continue

                results = await asyncio.gather(*[self.fetch_data(s) for s in self.symbols])
                positions_cache = await self.get_positions_cache()

                for i, symbol in enumerate(self.symbols):
                    try:
                        if symbol in self.cooldowns:
                            if datetime.now() < self.cooldowns[symbol]:
                                continue
                            del self.cooldowns[symbol]

                        price, df = results[i]
                        if price is None or df is None:
                            continue

                        score = self.score_calc.compute(df)
                        norm  = self._norm(symbol)
                        has_pos = norm in positions_cache

                        if score < self.sell_threshold:
                            self.bearish_count[symbol] = self.bearish_count.get(symbol, 0) + 1
                        else:
                            self.bearish_count[symbol] = 0

                        logger.info(
                            f"{symbol} | ${price:.4f} | score {score:.3f} | "
                            f"bearish_bars {self.bearish_count.get(symbol, 0)} | in_position: {has_pos}"
                        )

                        # ==================================================
                        # MANAGE OPEN POSITION
                        # ==================================================
                        if has_pos:
                            pos_data     = self.positions.get(symbol, {})
                            entry_price  = positions_cache[norm]['avg_price']
                            qty          = positions_cache[norm]['qty']
                            
                            # BACKSTOP PROTECTION: Calculate dynamic ATR backup targets if state cache was lost
                            atr_backup   = calc_atr(df, 14)
                            stop_price   = pos_data.get('stop_price',   entry_price - (atr_backup * self.atr_stop_mult))
                            target_price = pos_data.get('target_price', entry_price + (atr_backup * self.atr_target_mult))
                            bars_held    = pos_data.get('bars_held',    0)

                            # Re-populate local dictionary parameter tracks if file state went completely missing
                            if symbol not in self.positions:
                                self.positions[symbol] = {
                                    'entry_time':  datetime.now().isoformat(),
                                    'entry_price': entry_price,
                                    'stop_price':  stop_price,
                                    'target_price': target_price,
                                    'bars_held':    bars_held,
                                }

                            self.positions[symbol]['bars_held'] = bars_held + 1

                            exit_reason = None
                            if price <= stop_price:
                                exit_reason = "STOP_LOSS"
                            elif price >= target_price:
                                exit_reason = "TAKE_PROFIT"
                            elif (bars_held >= self.min_hold_bars and
                                  self.bearish_count.get(symbol, 0) >= self.sell_confirm_bars):
                                exit_reason = "SIGNAL_EXIT"

                            if exit_reason:
                                logger.info(f"  EXIT {symbol} — {exit_reason}")
                                success, fill_qty, fill_price = await self.submit_order(symbol, 'sell')
                                if success:
                                    pnl_usd = (fill_price - entry_price) * fill_qty
                                    self.total_pnl          += pnl_usd
                                    self.daily_pnl          += pnl_usd
                                    self.daily_trade_count += 1
                                    write_trade(
                                        symbol, 'SELL', fill_price, fill_qty,
                                        pnl_usd, self.total_pnl, score,
                                        exit_reason, stop_price, target_price
                                    )
                                    if symbol in self.positions:
                                        del self.positions[symbol]
                                    self.bearish_count[symbol] = 0

                                    if exit_reason == "STOP_LOSS":
                                        self.cooldowns[symbol] = datetime.now() + timedelta(hours=6)
                                        logger.info(f"  Cooldown set for {symbol} (6h, loss exit)")

                        # ==================================================
                        # LOOK FOR NEW ENTRY
                        # ==================================================
                        else:
                            if self.daily_trade_count >= self.max_daily_trades:
                                continue

                            if score > self.buy_threshold:
                                atr          = calc_atr(df, 14)
                                stop_price   = price - atr * self.atr_stop_mult
                                target_price = price + atr * self.atr_target_mult

                                logger.info(
                                    f"  BUY {symbol} @ ${price:.4f} | score {score:.3f} | "
                                    f"SL ${stop_price:.4f} | TP ${target_price:.4f}"
                                )
                                success, fill_qty, fill_price = await self.submit_order(
                                    symbol, 'buy', self.position_size_usd
                                )
                                if success:
                                    write_trade(
                                        symbol, 'BUY', fill_price, fill_qty,
                                        score=score, stop_price=stop_price, target_price=target_price
                                    )
                                    self.daily_trade_count += 1
                                    self.bearish_count[symbol] = 0
                                    self.positions[symbol] = {
                                        'entry_time':   datetime.now().isoformat(),
                                        'entry_price':  fill_price,
                                        'stop_price':   stop_price,
                                        'target_price': target_price,
                                        'bars_held':    0,
                                    }

                        self.save_state()

                    except Exception as e:
                        logger.error(f"{symbol} loop error: {e}")

                await asyncio.sleep(self.cycle_secs)

            except Exception as e:
                logger.error(f"Top-level error: {e}")
                await asyncio.sleep(30)

    def stop(self):
        self.save_state()
        logger.info(f"Shutdown. Total P&L: ${self.total_pnl:.2f}")

if __name__ == "__main__":
    bot = AlpacaTradingBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        bot.stop()

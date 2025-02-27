import pandas as pd
from ta.trend import MACD
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands
from websocket_trades import trade_queue
import logging
import time
import json
from datetime import datetime
from collections import deque
import sys
import threading
from queue import Queue
import importlib.util
import os

# Configure standard logger for web_dashboard.py
logger = logging.getLogger("web_dashboard")
logger.setLevel(logging.INFO)

# File handler for web_dashboard.log (standard logs)
fh = logging.FileHandler('web_dashboard.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Stream handler for console output
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

# Configure detailed logger for trade/book50 and indicator data
detailed_logger = logging.getLogger("web_dashboard.detailed")
detailed_logger.setLevel(logging.INFO)
detailed_logger.propagate = False  # Prevent propagation to root logger

# File handler for detailed logs (web_dashboard_detailed.log)
detailed_fh = logging.FileHandler('web_dashboard_detailed.log')
detailed_fh.setLevel(logging.INFO)
detailed_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s - '
    'Data: %(data_received)s - Time: %(time)s - Next: %(next_step)s'
)
detailed_fh.setFormatter(detailed_formatter)
detailed_logger.addHandler(detailed_fh)

# Queue to pass data to MySQL script
mysql_queue = Queue()

# Data storage
MAX_TRADES = 60
trades = deque(maxlen=MAX_TRADES)
latest_data = {'loaded': False}
latest_trade = {}
data_lock = threading.Lock()

def calculate_trend(df):
    if len(df) < 5:
        return "N/A"
    recent_prices = df['price'].tail(5)
    diff = recent_prices.diff().dropna()
    avg_change = diff.mean()
    if avg_change > 0.001:
        return "Up"
    elif avg_change < -0.001:
        return "Down"
    else:
        return "Flat"

def calculate_recommendations(df):
    latest = df.iloc[-1]
    current_price = latest['price']

    buy_score = 0
    sell_score = 0
    hold_score = 5

    rsi = latest['RSI']
    if rsi < 30: buy_score += 4
    elif rsi > 70: sell_score += 4

    macd_diff = latest['MACD_diff']
    if macd_diff > 0 and df['MACD_diff'].iloc[-2] <= 0: buy_score += 3
    elif macd_diff < 0 and df['MACD_diff'].iloc[-2] >= 0: sell_score += 3

    if current_price < latest['BB_lower']: buy_score += 3
    elif current_price > latest['BB_upper']: sell_score += 3

    stoch_k = latest['Stoch_K']
    if stoch_k < 20: buy_score += 3
    elif stoch_k > 80: sell_score += 3

    if latest['MA5'] > latest['MA10'] and df['MA5'].iloc[-2] <= df['MA10'].iloc[-2]: buy_score += 2
    elif latest['MA5'] < latest['MA10'] and df['MA5'].iloc[-2] >= df['MA10'].iloc[-2]: sell_score += 2

    buy_score = min(10, max(1, buy_score))
    sell_score = min(10, max(1, sell_score))
    hold_score = min(10, max(1, 10 - (buy_score + sell_score) // 2))

    return buy_score, sell_score, hold_score


def process_trades():
    global trades, latest_data, latest_trade
    first_print = True
    latest_book = None
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    logger.info("Starting trade processing...")
    while True:
        if not trade_queue.empty():
            data_type, data = trade_queue.get()
            if data_type == "trade":
                with data_lock:
                    trades.append(data)
                    latest_trade = data.copy()
                logger.info(f"Trade received: {data['symbol']} at {data['price']:.4f}")
                detailed_logger.info(
                    "Processing trade data",
                    extra={
                        'data_received': json.dumps(data),
                        'time': timestamp,
                        'next_step': 'Adding trade to trades deque for indicator calculation'
                    }
                )
            elif data_type == "book50":
                latest_book = data
                # ... (book50 processing unchanged) ...

        if len(trades) < 30:
            time.sleep(0.1)
            if first_print:
                sys.stdout.write("Waiting for initial data (30 trades required)...\n")
                sys.stdout.flush()
                first_print = False
            continue

        start = time.time()

        with data_lock:
            df = pd.DataFrame(list(trades))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # Fill NaN in raw data (just in case)
        df['price'] = df['price'].fillna(0.0)
        df['size'] = df['size'].fillna(0.0)

        # Calculate indicators
        df['MA5'] = df['price'].rolling(window=5, min_periods=5).mean().fillna(0.0)
        df['MA10'] = df['price'].rolling(window=10, min_periods=10).mean().fillna(0.0)
        df['MA15'] = df['price'].rolling(window=15, min_periods=15).mean().fillna(0.0)
        df['MA30'] = df['price'].rolling(window=30, min_periods=30).mean().fillna(0.0)

        macd = MACD(df['price'])
        df['MACD'] = macd.macd().fillna(0.0)
        df['MACD_signal'] = macd.macd_signal().fillna(0.0)
        df['MACD_diff'] = macd.macd_diff().fillna(0.0)

        df['Volume'] = df['size'].cumsum().fillna(0.0)

        rsi = RSIIndicator(df['price'], window=14)
        df['RSI'] = rsi.rsi().fillna(0.0)

        bb = BollingerBands(df['price'], window=20, window_dev=2)
        df['BB_upper'] = bb.bollinger_hband().fillna(0.0)
        df['BB_lower'] = bb.bollinger_lband().fillna(0.0)
        df['BB_middle'] = bb.bollinger_mavg().fillna(0.0)

        stochastic = StochasticOscillator(df['price'], df['price'], df['price'], window=14, smooth_window=3)
        df['Stoch_K'] = stochastic.stoch().fillna(0.0)
        df['Stoch_D'] = stochastic.stoch_signal().fillna(0.0)

        df['VWAP'] = ((df['price'] * df['size']).cumsum() / df['size'].cumsum()).fillna(0.0)

        spread = imbalance = 0.0
        if latest_book and latest_book['bids'] and latest_book['asks']:
            best_bid = max(latest_book['bids'], key=lambda x: x['price'])['price']
            best_ask = min(latest_book['asks'], key=lambda x: x['price'])['price']
            spread = best_ask - best_bid
            imbalance = sum(bid['size'] for bid in latest_book['bids'][:5]) / sum(ask['size'] for ask in latest_book['asks'][:5])

        latest = df.iloc[-1]
        buy, sell, hold = calculate_recommendations(df)
        trend = calculate_trend(df)

        with data_lock:
            processing_time = time.time() - start
            latest_data = {
                'loaded': True,
                'current_price': round(float(latest['price']), 8),
                'timestamp': latest['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'trend': trend,
                'buy': buy,
                'sell': sell,
                'hold': hold,
                'ma5': round(float(latest['MA5']), 8),
                'ma10': round(float(latest['MA10']), 8),
                'ma15': round(float(latest['MA15']), 8),
                'ma30': round(float(latest['MA30']), 8),
                'macd': round(float(latest['MACD']), 8),
                'macd_signal': round(float(latest['MACD_signal']), 8),
                'macd_diff': round(float(latest['MACD_diff']), 8),
                'volume': round(float(latest['Volume']), 8),
                'rsi': round(float(latest['RSI']), 8),
                'bb_upper': round(float(latest['BB_upper']), 8),
                'bb_middle': round(float(latest['BB_middle']), 8),
                'bb_lower': round(float(latest['BB_lower']), 8),
                'stoch_k': round(float(latest['Stoch_K']), 8),
                'stoch_d': round(float(latest['Stoch_D']), 8),
                'symbol': latest['symbol'],
                'processing_time': round(processing_time, 4),
                'vwap': round(float(latest['VWAP']), 8),
                'spread': round(spread, 8),
                'imbalance': round(imbalance, 8)
            }

            # Debug log to inspect data before queuing
            logger.debug(f"Prepared latest_data for MySQL: {json.dumps(latest_data)}")

        logger.info(f"Indicators calculated - Processing took {processing_time:.3f}s")
        # ... (detailed_logger unchanged) ...

        mysql_queue.put(latest_data)
        logger.info("Data queued to mysql_queue for MySQL storage")

        # ... (console output unchanged) ...

        output = (
            f"\rNew Trade - Symbol: {latest_data['symbol']:<20} Price: {latest_data['current_price']:<10.4f} Size: {latest['size']:<10.2f}\n"
            f"Dashboard Data:\n"
            f"  Price    : {latest_data['current_price']:<10.4f}  Trend: {latest_data['trend']:<5}\n"
            f"  Buy      : {latest_data['buy']:<3}  Sell: {latest_data['sell']:<3}  Hold: {latest_data['hold']:<3}\n"
            f"  MA5      : {latest_data['ma5']:<10.4f}  MA10: {latest_data['ma10']:<10.4f}\n"
            f"  MA15     : {latest_data['ma15']:<10.4f}  MA30: {latest_data['ma30']:<10.4f}\n"
            f"  MACD     : {latest_data['macd']:<10.4f}  Signal: {latest_data['macd_signal']:<10.4f}  Diff: {latest_data['macd_diff']:<10.4f}\n"
            f"  Volume   : {latest_data['volume']:<10.2f}\n"
            f"  RSI      : {latest_data['rsi']:<10.2f}\n"
            f"  BB Upper : {latest_data['bb_upper']:<10.4f}  Middle: {latest_data['bb_middle']:<10.4f}  Lower: {latest_data['bb_lower']:<10.4f}\n"
            f"  Stoch %K : {latest_data['stoch_k']:<10.2f}  %D: {latest_data['stoch_d']:<10.2f}\n"
            f"  VWAP     : {latest_data['vwap']:<10.4f}\n"
            f"  Spread   : {latest_data['spread']:<10.4f}  Imbalance: {latest_data['imbalance']:<10.2f}\n"
            f"  Timestamp: {latest_data['timestamp']}"
        )
        sys.stdout.write(output)
        sys.stdout.flush()
        time.sleep(0.1)




if __name__ == "__main__":
    logger.info("Starting trade processing...")
    trade_thread = threading.Thread(target=process_trades, daemon=True)
    trade_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down trade processing...")
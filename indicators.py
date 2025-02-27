import pandas as pd
import numpy as np
from ta.trend import MACD
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands
import time
from websocket_trades import trade_queue  # Import queue from WebSocket script
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Data storage
trades = []

def calculate_indicators():
    """Calculate technical indicators from trade data."""
    global trades
    while True:
        # Get new trade from queue
        if not trade_queue.empty():
            trade = trade_queue.get()
            trades.append(trade)
            logger.info(f"Added trade: {trade}")

        # Need at least 30 trades for MA30
        if len(trades) < 30:
            time.sleep(1)
            continue

        # Convert to DataFrame
        df = pd.DataFrame(trades)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # Calculate Moving Averages
        df['MA5'] = df['price'].rolling(window=5).mean()
        df['MA10'] = df['price'].rolling(window=10).mean()
        df['MA15'] = df['price'].rolling(window=15).mean()
        df['MA30'] = df['price'].rolling(window=30).mean()

        # Calculate MACD
        macd = MACD(df['price'])
        df['MACD'] = macd.macd()
        df['MACD_signal'] = macd.macd_signal()
        df['MACD_diff'] = macd.macd_diff()

        # Calculate Volume (cumulative size)
        df['Volume'] = df['size'].cumsum()

        # Calculate RSI
        rsi = RSIIndicator(df['price'], window=14)
        df['RSI'] = rsi.rsi()

        # Calculate Bollinger Bands
        bb = BollingerBands(df['price'], window=20, window_dev=2)
        df['BB_upper'] = bb.bollinger_hband()
        df['BB_lower'] = bb.bollinger_lband()
        df['BB_middle'] = bb.bollinger_mavg()

        # Calculate Stochastic Oscillator
        stochastic = StochasticOscillator(df['price'], df['price'], df['price'], window=14, smooth_window=3)
        df['Stoch_K'] = stochastic.stoch()
        df['Stoch_D'] = stochastic.stoch_signal()

        # Display latest indicators
        latest = df.iloc[-1]
        logger.info(f"Latest Indicators:")
        logger.info(f"  MA5: {latest['MA5']:.4f}, MA10: {latest['MA10']:.4f}, MA15: {latest['MA15']:.4f}, MA30: {latest['MA30']:.4f}")
        logger.info(f"  MACD: {latest['MACD']:.4f}, Signal: {latest['MACD_signal']:.4f}, Diff: {latest['MACD_diff']:.4f}")
        logger.info(f"  Volume: {latest['Volume']:.2f}")
        logger.info(f"  RSI: {latest['RSI']:.2f}")
        logger.info(f"  Bollinger Bands - Upper: {latest['BB_upper']:.4f}, Middle: {latest['BB_middle']:.4f}, Lower: {latest['BB_lower']:.4f}")
        logger.info(f"  Stochastic - %K: {latest['Stoch_K']:.2f}, %D: {latest['Stoch_D']:.2f}")

        time.sleep(1)  # Wait before next calculation

if __name__ == "__main__":
    try:
        calculate_indicators()
    except KeyboardInterrupt:
        logger.info("Shutting down indicators...")
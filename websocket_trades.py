import websocket
import json
import time
import logging
from threading import Thread
from queue import Queue
import os

# Configure logging for websocket_trades.py
logger = logging.getLogger("websocket_trades")
logger.setLevel(logging.INFO)

# Create a standard formatter for regular logs
standard_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create a detailed formatter for trade/book50 logs
detailed_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s - '
    'Data: %(data_received)s - Time: %(time)s - Next: %(next_step)s'
)

# File handler for detailed trade/book50 logs
try:
    log_file_path = os.path.abspath('websocket_trades.log')
    print(f"Attempting to create log file at: {log_file_path}")
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(logging.INFO)
    fh.setFormatter(standard_formatter)  # Use standard formatter by default
    logger.addHandler(fh)
    print(f"Successfully set up file handler for logging to: {log_file_path}")
except Exception as e:
    print(f"Failed to set up file handler for logging: {e}")

# Stream handler for console with simple format
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(standard_formatter)
logger.addHandler(sh)

# Create a separate logger for detailed logging
detailed_logger = logging.getLogger("websocket_trades.detailed")
detailed_logger.setLevel(logging.INFO)
# Make sure detailed logger doesn't propagate to root logger
detailed_logger.propagate = False

# Add file handler for detailed logs
try:
    detailed_fh = logging.FileHandler(os.path.abspath('websocket_detailed.log'))
    detailed_fh.setLevel(logging.INFO)
    detailed_fh.setFormatter(detailed_formatter)
    detailed_logger.addHandler(detailed_fh)
except Exception as e:
    print(f"Failed to set up detailed file handler: {e}")

# WebSocket URL and API key
WS_URL = "wss://ws.coinapi.io/v1/"
API_KEY = "15e7109f-ca51-4e51-b18f-ad9f63903538"  # Replace with your CoinAPI key

# Subscription message updated to include "book50"
SUBSCRIPTION = {
    "type": "hello",
    "apikey": API_KEY,
    "heartbeat": True,
    "subscribe_data_type": ["trade", "book50"],
    "subscribe_filter_asset_id": ["PI"],
    "subscribe_filter_exchange_id": ["BITGET"]
}

# Queue to store trade and book50 data for processing
trade_queue = Queue()

def on_message(ws, message):
    """Handle incoming WebSocket messages."""
    try:
        data = json.loads(message)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        if data.get("type") == "trade":
            trade = {
                "symbol": data.get("symbol_id", "N/A"),
                "price": float(data.get("price", 0)),
                "size": float(data.get("size", 0)),
                "timestamp": data.get("time_exchange", "N/A")
            }
            # Log regular message
            logger.info(f"Received trade data: {json.dumps(trade)}")
            
            # Log detailed message
            detailed_logger.info(
                "Processing trade data",
                extra={
                    'data_received': json.dumps(trade),
                    'time': timestamp,
                    'next_step': 'Passing trade data to trade_queue for processing'
                }
            )
            
            trade_queue.put(("trade", trade))  # Add type to distinguish
            
            # Log regular message
            logger.info("Trade data queued successfully")
            
            # Log detailed message
            detailed_logger.info(
                "Trade data processed",
                extra={
                    'data_received': json.dumps(trade),
                    'time': timestamp,
                    'next_step': 'Trade data passed to trade_queue for processing'
                }
            )
            
        elif data.get("type") == "book50":
            book = {
                "symbol": data.get("symbol_id", "N/A"),
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
                "timestamp": data.get("time_exchange", "N/A")
            }
            # Log regular message
            logger.info(f"Received book50 data: {json.dumps(book)}")
            
            # Log detailed message
            detailed_logger.info(
                "Processing book50 data",
                extra={
                    'data_received': json.dumps(book),
                    'time': timestamp,
                    'next_step': 'Passing book50 data to trade_queue for processing'
                }
            )
            
            trade_queue.put(("book50", book))  # Add type to distinguish
            
            # Log regular message
            logger.info("Book50 data queued successfully")
            
            # Log detailed message
            detailed_logger.info(
                "Book50 data processed",
                extra={
                    'data_received': json.dumps(book),
                    'time': timestamp,
                    'next_step': 'Book50 data passed to trade_queue for processing'
                }
            )
            
        elif data.get("type") == "error":
            logger.error(f"Error from server: {data.get('message', 'Unknown error')}")
            ws.close()
        elif data.get("type") == "heartbeat":
            logger.debug("Heartbeat received")
        else:
            logger.info(f"Other message type: {message}")
    except json.JSONDecodeError:
        logger.error(f"Failed to parse message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info(f"Connection closed - Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    logger.info("WebSocket connection opened")
    ws.send(json.dumps(SUBSCRIPTION))

def run_websocket():
    """Run the WebSocket connection with reconnection logic."""
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"WebSocket crashed: {e}")
        logger.info("Attempting to reconnect in 5 seconds...")
        time.sleep(5)

if __name__ == "__main__":
    ws_thread = Thread(target=run_websocket, daemon=True)
    ws_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
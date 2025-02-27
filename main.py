import time
import threading
import logging
from websocket_trades import run_websocket
from web_dashboard import process_trades
from mysql_storage import process_mysql

# Configure logging for main.py
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)

# File handler for main.log
fh = logging.FileHandler('main.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Stream handler for console output
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

def main():
    """Run websocket_trades.py, web_dashboard.py, and mysql_storage.py together."""
    # Thread for WebSocket connection
    ws_thread = threading.Thread(target=run_websocket, daemon=True, name="WebSocketThread")
    # Thread for trade processing
    trade_thread = threading.Thread(target=process_trades, daemon=True, name="TradeProcessingThread")
    # Thread for MySQL storage
    mysql_thread = threading.Thread(target=process_mysql, daemon=True, name="MySQLThread")

    logger.info("Starting crypto trading bot...")
    ws_thread.start()
    logger.info("WebSocket thread started")
    trade_thread.start()
    logger.info("Trade processing thread started")
    mysql_thread.start()
    logger.info("MySQL storage thread started")

    try:
        while True:
            time.sleep(1)
            logger.debug("Main thread running...")
    except KeyboardInterrupt:
        logger.info("Shutting down application...")
        # Threads are daemon, so they will terminate when main thread exits
    except Exception as e:
        logger.error(f"Error in main thread: {e}")

if __name__ == "__main__":
    main()
import time
import threading
import logging
from websocket_trades import run_websocket
from web_dashboard import process_trades
from mysql_storage import process_mysql
from fetch_data import fetch_data
from organize_data import organize_data
from send_to_n8n import send_to_n8n

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

def run_data_pipeline():
    """Run the data fetching, organizing, and sending to n8n in a loop."""
    while True:
        try:
            fetch_data()
            organize_data()
            send_to_n8n()
            logger.info("Data pipeline cycle completed (fetch -> organize -> send)")
            time.sleep(60)  # Run every 60 seconds
        except Exception as e:
            logger.error(f"Error in data pipeline: {e}")
            time.sleep(10)  # Wait before retrying on error

def main():
    """Run all scripts together: websocket, dashboard, mysql, and data pipeline."""
    # Thread for WebSocket connection
    ws_thread = threading.Thread(target=run_websocket, daemon=True, name="WebSocketThread")
    # Thread for trade processing
    trade_thread = threading.Thread(target=process_trades, daemon=True, name="TradeProcessingThread")
    # Thread for MySQL storage
    mysql_thread = threading.Thread(target=process_mysql, daemon=True, name="MySQLThread")
    # Thread for data pipeline (fetching, organizing, sending to n8n)
    data_pipeline_thread = threading.Thread(target=run_data_pipeline, daemon=True, name="DataPipelineThread")

    logger.info("Starting crypto trading bot with all components...")
    ws_thread.start()
    logger.info("WebSocket thread started")
    trade_thread.start()
    logger.info("Trade processing thread started")
    mysql_thread.start()
    logger.info("MySQL storage thread started")
    data_pipeline_thread.start()
    logger.info("Data pipeline thread started (fetch -> organize -> send to n8n)")

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
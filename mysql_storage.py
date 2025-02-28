import json
from queue import Queue, Empty  # Import Empty from queue module
import mysql.connector
import time
import logging
from datetime import datetime
from threading import Thread

import pytz

# Create a queue in this module since it's not defined in web_dashboard
from web_dashboard import mysql_queue

# Configure standard logger for mysql_storage.py
logger = logging.getLogger("mysql_storage")
logger.setLevel(logging.INFO)

# File handler for mysql_storage.log (standard logs)
fh = logging.FileHandler('mysql_storage.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Stream handler for console output
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

# Configure detailed logger for data-related messages
detailed_logger = logging.getLogger("mysql_storage.detailed")
detailed_logger.setLevel(logging.INFO)
detailed_logger.propagate = False  # Prevent propagation to root logger

# File handler for detailed logs (mysql_storage_detailed.log)
detailed_fh = logging.FileHandler('mysql_storage_detailed.log')
detailed_fh.setLevel(logging.INFO)
detailed_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s - '
    'Data: %(data_received)s - Time: %(time)s - Next: %(next_step)s'
)
detailed_fh.setFormatter(detailed_formatter)
detailed_logger.addHandler(detailed_fh)

# MySQL connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin',  # Replace with your MySQL password
    'database': 'trading_db',
    'raise_on_warnings': True
}

# Initialize MySQL connection
conn = None
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pi_trades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(255),
            current_price DECIMAL(20, 8),
            timestamp DATETIME(6),
            trend VARCHAR(10),
            buy_score INT,
            sell_score INT,
            hold_score INT,
            ma5 DECIMAL(20, 8),
            ma10 DECIMAL(20, 8),
            ma15 DECIMAL(20, 8),
            ma30 DECIMAL(20, 8),
            macd DECIMAL(20, 8),
            macd_signal DECIMAL(20, 8),
            macd_diff DECIMAL(20, 8),
            volume DECIMAL(20, 8),
            rsi DECIMAL(20, 8),
            bb_upper DECIMAL(20, 8),
            bb_middle DECIMAL(20, 8),
            bb_lower DECIMAL(20, 8),
            stoch_k DECIMAL(20, 8),
            stoch_d DECIMAL(20, 8),
            vwap DECIMAL(20, 8),
            spread DECIMAL(20, 8),
            imbalance DECIMAL(20, 8),
            processing_time DECIMAL(10, 4)
        )
    """)
    conn.commit()
    logger.info("Connected to MySQL and created table pi_trades")
except mysql.connector.Error as err:
    logger.error(f"Failed to connect to MySQL: {err}")

def store_data(data):
    if not conn:
        logger.error("No MySQL connection available")
        return False
    try:
        utc_time = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        timestamp_dt = utc_time.replace(tzinfo=pytz.UTC)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pi_trades (
                symbol, current_price, timestamp, trend, buy_score, sell_score, hold_score,
                ma5, ma10, ma15, ma30, macd, macd_signal, macd_diff, volume, rsi,
                bb_upper, bb_middle, bb_lower, stoch_k, stoch_d, vwap, spread, imbalance,
                processing_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['symbol'],
            data['current_price'],
            timestamp_dt,
            data['trend'],
            data['buy'],
            data['sell'],
            data['hold'],
            data['ma5'],
            data['ma10'],
            data['ma15'],
            data['ma30'],
            data['macd'],
            data['macd_signal'],
            data['macd_diff'],
            data['volume'],
            data['rsi'],
            data['bb_upper'],
            data['bb_middle'],
            data['bb_lower'],
            data['stoch_k'],
            data['stoch_d'],
            data['vwap'],
            data['spread'],
            data['imbalance'],
            data['processing_time']
        ))
        conn.commit()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        logger.info(f"Stored data for {data['symbol']} at {data['timestamp']}")
        detailed_logger.info(
            "Stored data in MySQL",
            extra={
                'data_received': json.dumps(data),
                'time': timestamp,
                'next_step': 'Data available for n8n/AI processing'
            }
        )
        return True
    except mysql.connector.Error as err:
        logger.error(f"Failed to store data in MySQL: {err}")
        if not conn.is_connected():
            logger.warning("MySQL connection lost. Attempting to reconnect...")
            try:
                conn.reconnect(attempts=3, delay=5)
                logger.info("Reconnected to MySQL successfully")
            except mysql.connector.Error as reconnect_err:
                logger.error(f"Failed to reconnect to MySQL: {reconnect_err}")
        return False
    except ValueError as ve:
        logger.error(f"Timestamp parsing error: {ve}")
        return False

def process_mysql():
    logger.info("Starting MySQL storage...")
    while True:
        try:
            # Use a timeout to avoid blocking indefinitely
            data = mysql_queue.get(timeout=5)
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            logger.info(f"Received data for storage: {data['symbol']} at {data['timestamp']}")
            detailed_logger.info(
                "Received data for storage",
                extra={
                    'data_received': json.dumps(data),
                    'time': timestamp,
                    'next_step': 'Storing data in MySQL'
                }
            )
            if store_data(data):
                logger.info("Data processed and stored in MySQL")
            else:
                logger.warning("Failed to store data in MySQL")
        except Empty:  # Use the proper exception name
            # This is expected behavior when the queue is empty
            time.sleep(1)
        except Exception as e:
            logger.error(f"Error processing MySQL storage: {e}")
            time.sleep(1)

# Function to add data to the queue (to be called from web_dashboard.py)
def add_to_mysql_queue(data):
    mysql_queue.put(data)
    logger.info(f"Added data to MySQL queue for {data['symbol']}")

if __name__ == "__main__":
    logger.info("Starting MySQL process...")
    mysql_thread = Thread(target=process_mysql, daemon=True)
    mysql_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down MySQL storage...")
        if conn:
            conn.close()
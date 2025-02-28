from decimal import Decimal
import mysql.connector
import json
import logging
from datetime import datetime, timedelta

import pytz

# Configure logging for fetch_data.py
logger = logging.getLogger("fetch_data")
logger.setLevel(logging.INFO)

# File handler for fetch_data.log
fh = logging.FileHandler('fetch_data.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Stream handler for console output
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "admin",
    "database": "trading_db"
}

def fetch_data():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Define time range (last 5 minutes)
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(minutes=1)
        
        # Query the pi_trades table
        query = """
        SELECT timestamp, current_price, symbol, trend, buy_score, sell_score, hold_score,
            ma5, ma10, ma15, ma30, macd, macd_signal, macd_diff, volume, rsi,
            bb_upper, bb_middle, bb_lower, stoch_k, stoch_d, vwap, spread, imbalance
        FROM pi_trades
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
        """
        cursor.execute(query, (start_time, end_time))
        
        # Fetch all rows
        data = cursor.fetchall()
        logger.info(f"Retrieved {len(data)} rows")
        for row in data:
            if isinstance(row['timestamp'], datetime):
                row['timestamp'] = row['timestamp'].isoformat()
            for key in row:
                if isinstance(row[key], Decimal):
                    row[key] = float(row[key])    
        # Close connection
        cursor.close()
        conn.close()
        
        # Save to JSON file
        with open("recent_data.json", "w") as f:
            json.dump(data, f)
        logger.info(f"Fetched {len(data)} records from MySQL and saved to recent_data.json")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")

if __name__ == "__main__":
    fetch_data()
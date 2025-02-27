import mysql.connector
import json
from datetime import datetime, timedelta

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "admin",
    "database": "trading_db"
}

def fetch_data():
    # Connect to MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # Define time range (last 5 minutes)
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=5)
    
    # Query the pi_trades table
    query = """
    SELECT timestamp, current_price, ma50, rsi
    FROM pi_trades
    WHERE timestamp BETWEEN %s AND %s
    ORDER BY timestamp ASC
    """
    cursor.execute(query, (start_time, end_time))
    
    # Fetch all rows
    data = cursor.fetchall()
    
    # Close connection
    cursor.close()
    conn.close()
    
    # Save to JSON file
    with open("recent_data.json", "w") as f:
        json.dump(data, f)
    print("Data fetched and saved to recent_data.json")

if __name__ == "__main__":
    fetch_data()
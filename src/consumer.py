import json
import sqlite3
from datetime import datetime
import pytz
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import requests

# Load environment variables
load_dotenv()

# Timezone
JAKARTA_TZ = pytz.timezone('Asia/Jakarta')

# Redpanda config
REDPANDA_BROKER = os.getenv('REDPANDA_BROKER', 'localhost:9092')
TOPIC = 'crypto-prices'

# SQLite config
DB_PATH = 'crypto.db'
TABLE_NAME = 'crypto_prices'

# Slack config
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL')
PRICE_THRESHOLD = 5.0

def init_sqlite():
    """Initialize SQLite dan create table if not exists"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create table if not exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            symbol TEXT,
            price REAL,
            currency TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    print(f"✓ SQLite initialized (database: {DB_PATH})")
    return conn

def get_previous_price(conn, symbol):
    """Get second-latest price for comparison (previous price)"""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT price FROM {TABLE_NAME} 
        WHERE symbol = ? 
        ORDER BY timestamp DESC 
        LIMIT 2 OFFSET 1
    """, (symbol,))
    result = cursor.fetchone()
    return result[0] if result else None

def calculate_change_percent(current, previous):
    """Calculate percentage change"""
    if previous == 0 or previous is None:
        return 0
    return abs((current - previous) / previous) * 100

def send_slack_alert(symbol, current_price, previous_price, change_percent):
    """Send alert to Slack"""
    if not SLACK_WEBHOOK:
        return
    
    direction = "📈" if current_price > previous_price else "📉"
    message = {
        "text": f"{direction} {symbol} Alert!",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{symbol} Price Alert*\n{direction} *{change_percent:.2f}%* change detected\n\nPrevious: ${previous_price:.2f}\nCurrent: ${current_price:.2f}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"_{datetime.now(JAKARTA_TZ).strftime('%Y-%m-%d %H:%M:%S')}_"
                    }
                ]
            }
        ]
    }
    
    try:
        response = requests.post(SLACK_WEBHOOK, json=message)
        if response.status_code == 200:
            print(f"  🔔 Slack alert sent for {symbol}")
    except Exception as e:
        print(f"  ✗ Slack error: {e}")

def insert_message(conn, message_json):
    """Parse message from Redpanda and insert into SQLite"""
    try:
        data = json.loads(message_json)
        symbol = data['symbol']
        price = data['price']
        
        cursor = conn.cursor()
        # Insert to SQLite
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (timestamp, symbol, price, currency)
            VALUES (?, ?, ?, ?)
        """, (
            data['timestamp'],
            symbol,
            price,
            data['currency']
        ))
        
        conn.commit()
        print(f"✓ Inserted {symbol}: \${price}")
        
        # Check price alert
        previous_price = get_previous_price(conn, symbol)
        if previous_price:
            change = calculate_change_percent(price, previous_price)
            if change >= PRICE_THRESHOLD:
                print(f"  🚨 ALERT: {symbol} changed {change:.2f}%!")
                send_slack_alert(symbol, price, previous_price, change)
    
    except Exception as e:
        print(f"ERROR: Failed to insert message: {e}")

def main():
    """Main consumer loop"""
    print(f"Starting Crypto Consumer...")
    print(f"Broker: {REDPANDA_BROKER}")
    print(f"Topic: {TOPIC}")
    print(f"Database: {DB_PATH}")
    if SLACK_WEBHOOK:
        print(f"Slack alerts: ENABLED")
    else:
        print(f"Slack alerts: DISABLED (no webhook URL)\n")
    
    # Initialize SQLite
    conn = init_sqlite()
    
    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[REDPANDA_BROKER],
            group_id='crypto-consumer-group',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: m.decode('utf-8')
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to Redpanda: {e}")
        return
    
    try:
        print(f"Listening for messages...\n")
        for message in consumer:
            print(f"[{datetime.now(JAKARTA_TZ).isoformat()}] Received message")
            insert_message(conn, message.value)
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        consumer.close()
        conn.close()
        print("Consumer and database closed.")

if __name__ == '__main__':
    main()
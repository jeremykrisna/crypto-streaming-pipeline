import json
import sqlite3
from datetime import datetime
import pytz
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

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

def insert_message(conn, message_json):
    """Parse message from Redpanda and insert into SQLite"""
    try:
        data = json.loads(message_json)
        
        cursor = conn.cursor()
        # Insert to SQLite
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (timestamp, symbol, price, currency)
            VALUES (?, ?, ?, ?)
        """, (
            data['timestamp'],
            data['symbol'],
            data['price'],
            data['currency']
        ))
        
        conn.commit()
        print(f"✓ Inserted {data['symbol']}: \${data['price']}")
    
    except Exception as e:
        print(f"ERROR: Failed to insert message: {e}")

def main():
    """Main consumer loop"""
    print(f"Starting Crypto Consumer...")
    print(f"Broker: {REDPANDA_BROKER}")
    print(f"Topic: {TOPIC}")
    print(f"Database: {DB_PATH}\n")
    
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
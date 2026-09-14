import json
import time
import requests
from datetime import datetime
import pytz
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Load environment variables
load_dotenv()

# Timezone
JAKARTA_TZ = pytz.timezone('Asia/Jakarta')

# Redpanda config
import socket
try:
    socket.gethostbyname('redpanda-broker')
    REDPANDA_BROKER = os.getenv('REDPANDA_BROKER', 'redpanda-broker:9092')
except:
    REDPANDA_BROKER = os.getenv('REDPANDA_BROKER', 'localhost:9092')
    
TOPIC = 'crypto-prices'

# CoinGecko API
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3/simple/price'
CRYPTO_IDS = ['bitcoin', 'ethereum', 'solana']

def fetch_crypto_prices():
    """Fetch crypto prices from CoinGecko API"""
    try:
        params = {
            'ids': ','.join(CRYPTO_IDS),
            'vs_currencies': 'usd',
            'include_market_cap': 'false',
            'include_24hr_vol': 'false',
            'include_last_updated_at': 'true'
        }
        
        response = requests.get(
            COINGECKO_API_URL, 
            params=params, 
            timeout=10,
            verify=False
        )
        response.raise_for_status()
        return response.json()
    
    except requests.RequestException as e:
        print(f"ERROR: Failed to fetch from CoinGecko: {e}")
        return None

def publish_prices(producer, prices_data):
    """Publish prices to Redpanda"""
    if not prices_data:
        return
    
    crypto_mapping = {
        'bitcoin': 'BTC',
        'ethereum': 'ETH',
        'solana': 'SOL'
    }
    
    timestamp = datetime.now(JAKARTA_TZ).isoformat()
    
    for crypto_id, symbol in crypto_mapping.items():
        if crypto_id in prices_data:
            price = prices_data[crypto_id].get('usd')
            
            message = {
                'timestamp': timestamp,
                'symbol': symbol,
                'price': price,
                'currency': 'usd'
            }
            
            try:
                producer.send(
                    TOPIC,
                    value=json.dumps(message).encode('utf-8')
                )
                print(f"✓ Published {symbol}: ${price} at {timestamp}")
            
            except Exception as e:
                print(f"ERROR: Failed to publish {symbol}: {e}")

def main():
    """Main producer loop"""
    print(f"Starting Crypto Producer...")
    print(f"Broker: {REDPANDA_BROKER}")
    print(f"Topic: {TOPIC}")
    print(f"Fetching prices every 10 seconds (Jakarta time)...\n")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[REDPANDA_BROKER],
            value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else v
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to Redpanda: {e}")
        return
    
    try:
        while True:
            print(f"\n[{datetime.now(JAKARTA_TZ).isoformat()}] Fetching prices...")
            prices = fetch_crypto_prices()
            publish_prices(producer, prices)
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == '__main__':
    main()
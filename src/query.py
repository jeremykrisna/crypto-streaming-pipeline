import sqlite3
from datetime import datetime
import pytz
from tabulate import tabulate

# Timezone
JAKARTA_TZ = pytz.timezone('Asia/Jakarta')
DB_PATH = 'crypto.db'
TABLE_NAME = 'crypto_prices'

def get_latest_prices():
    """Get latest price for each symbol"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = f"""
    SELECT 
        symbol,
        price,
        timestamp,
        COUNT(*) as total_records
    FROM {TABLE_NAME}
    WHERE timestamp = (
        SELECT MAX(timestamp) FROM {TABLE_NAME}
    )
    GROUP BY symbol
    ORDER BY symbol
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    if rows:
        headers = ['Symbol', 'Price (USD)', 'Timestamp', 'Total Records']
        print("\n📊 LATEST PRICES")
        print("=" * 80)
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print("No data available")

def get_statistics():
    """Get price statistics (avg, min, max) per symbol"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = f"""
    SELECT 
        symbol,
        COUNT(*) as count,
        ROUND(AVG(price), 2) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        ROUND(MAX(price) - MIN(price), 2) as price_range
    FROM {TABLE_NAME}
    GROUP BY symbol
    ORDER BY symbol
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    if rows:
        headers = ['Symbol', 'Count', 'Avg Price', 'Min Price', 'Max Price', 'Range']
        print("\n📈 PRICE STATISTICS")
        print("=" * 100)
        print(tabulate(rows, headers=headers, tablefmt='grid', floatfmt='.2f'))
    else:
        print("No data available")

def get_hourly_aggregation():
    """Get hourly price aggregation"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = f"""
    SELECT 
        symbol,
        datetime(timestamp, 'start of hour') as hour,
        COUNT(*) as count,
        ROUND(AVG(price), 2) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price
    FROM {TABLE_NAME}
    GROUP BY symbol, datetime(timestamp, 'start of hour')
    ORDER BY symbol, hour DESC
    LIMIT 20
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    if rows:
        headers = ['Symbol', 'Hour (UTC)', 'Count', 'Avg Price', 'Min Price', 'Max Price']
        print("\n⏰ HOURLY AGGREGATION (Latest 20 records)")
        print("=" * 120)
        print(tabulate(rows, headers=headers, tablefmt='grid', floatfmt='.2f'))
    else:
        print("No data available")

def get_total_records():
    """Get total records count"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(DISTINCT symbol) FROM {TABLE_NAME}")
    symbols = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n📋 DATABASE INFO")
    print("=" * 40)
    print(f"Total Records: {total}")
    print(f"Unique Symbols: {symbols}")

def main():
    """Run all queries"""
    print(f"\n{'='*80}")
    print(f"CRYPTO PRICES QUERY REPORT - {datetime.now(JAKARTA_TZ).isoformat()}")
    print(f"{'='*80}")
    
    get_total_records()
    get_latest_prices()
    get_statistics()
    get_hourly_aggregation()
    
    print(f"\n{'='*80}\n")

if __name__ == '__main__':
    main()
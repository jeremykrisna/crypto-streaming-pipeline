import sqlite3
from datetime import datetime, timedelta
import pytz
from tabulate import tabulate

# Timezone
JAKARTA_TZ = pytz.timezone('Asia/Jakarta')

# SQLite config
DB_PATH = 'crypto.db'
TABLE_NAME = 'crypto_prices'

def get_connection():
    """Get SQLite connection"""
    return sqlite3.connect(DB_PATH)

def get_latest_prices():
    """Get latest price for each symbol"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT symbol, price, timestamp 
        FROM {TABLE_NAME}
        WHERE (symbol, timestamp) IN (
            SELECT symbol, MAX(timestamp)
            FROM {TABLE_NAME}
            GROUP BY symbol
        )
        ORDER BY symbol
    """)
    
    results = cursor.fetchall()
    conn.close()
    return results

def get_price_stats():
    """Get price statistics (min, max, avg, range)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT 
            symbol,
            ROUND(MIN(price), 2) as min_price,
            ROUND(MAX(price), 2) as max_price,
            ROUND(AVG(price), 2) as avg_price,
            ROUND(MAX(price) - MIN(price), 2) as range_price,
            COUNT(*) as data_points
        FROM {TABLE_NAME}
        GROUP BY symbol
        ORDER BY symbol
    """)
    
    results = cursor.fetchall()
    conn.close()
    return results

def get_moving_averages():
    """Get 1-hour and 4-hour moving averages"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get current time and calculate time windows
    now = datetime.now(JAKARTA_TZ)
    one_hour_ago = now - timedelta(hours=1)
    four_hours_ago = now - timedelta(hours=4)
    
    cursor.execute(f"""
        SELECT 
            symbol,
            ROUND(AVG(CASE WHEN timestamp >= ? THEN price END), 2) as ma_1h,
            ROUND(AVG(CASE WHEN timestamp >= ? THEN price END), 2) as ma_4h,
            COUNT(CASE WHEN timestamp >= ? THEN 1 END) as count_1h,
            COUNT(CASE WHEN timestamp >= ? THEN 1 END) as count_4h
        FROM {TABLE_NAME}
        GROUP BY symbol
        ORDER BY symbol
    """, (
        one_hour_ago.isoformat(),
        four_hours_ago.isoformat(),
        one_hour_ago.isoformat(),
        four_hours_ago.isoformat()
    ))
    
    results = cursor.fetchall()
    conn.close()
    return results

def get_hourly_aggregation():
    """Get hourly price aggregation"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT 
            symbol,
            SUBSTR(timestamp, 1, 13) || ':00:00' as hour,
            ROUND(AVG(price), 2) as avg_price,
            ROUND(MIN(price), 2) as min_price,
            ROUND(MAX(price), 2) as max_price,
            COUNT(*) as count
        FROM {TABLE_NAME}
        GROUP BY symbol, SUBSTR(timestamp, 1, 13)
        ORDER BY symbol, hour DESC
        LIMIT 12
    """)
    
    results = cursor.fetchall()
    conn.close()
    return results

def print_latest_prices():
    """Print latest prices"""
    results = get_latest_prices()
    if not results:
        print("No data available")
        return
    
    print("\n📊 LATEST PRICES")
    print("-" * 60)
    headers = ["Symbol", "Price (USD)", "Timestamp"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

def print_price_stats():
    """Print price statistics"""
    results = get_price_stats()
    if not results:
        print("No data available")
        return
    
    print("\n📈 PRICE STATISTICS")
    print("-" * 80)
    headers = ["Symbol", "Min", "Max", "Avg", "Range", "Data Points"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

def print_moving_averages():
    """Print moving averages"""
    results = get_moving_averages()
    if not results:
        print("No data available")
        return
    
    print("\n🔄 MOVING AVERAGES")
    print("-" * 80)
    headers = ["Symbol", "1-Hour MA", "4-Hour MA", "Points (1h)", "Points (4h)"]
    data = [
        (r[0], r[1], r[2], r[3], r[4]) for r in results
    ]
    print(tabulate(data, headers=headers, tablefmt="grid"))

def print_hourly_aggregation():
    """Print hourly aggregation"""
    results = get_hourly_aggregation()
    if not results:
        print("No data available")
        return
    
    print("\n⏰ HOURLY AGGREGATION (Last 12 Hours)")
    print("-" * 90)
    headers = ["Symbol", "Hour", "Avg Price", "Min Price", "Max Price", "Count"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

def main():
    """Main query function"""
    print(f"\n{'='*90}")
    print(f"CRYPTO STREAMING PIPELINE - ANALYTICS")
    print(f"Time: {datetime.now(JAKARTA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {DB_PATH}")
    print(f"{'='*90}")
    
    print_latest_prices()
    print_price_stats()
    print_moving_averages()
    print_hourly_aggregation()
    
    print(f"\n{'='*90}\n")

if __name__ == '__main__':
    main()
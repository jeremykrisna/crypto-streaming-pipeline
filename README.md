Real-Time Crypto Streaming Pipeline

A production-grade, end-to-end data streaming pipeline that ingests cryptocurrency prices in real-time, demonstrates Kafka-style message brokering, provides real-time price alerts, and includes monitoring dashboards.

Architecture
CoinGecko API
    ↓
Producer (Python)
    ↓
Redpanda (Message Broker)
    ↓
Consumer (Python) + Slack Alerts
    ↓
SQLite (Data Storage)
    ↓
Query Analytics (Python) + Moving Averages
    ↓
Prometheus/Grafana (Monitoring & Dashboards)
Tech Stack
Language: Python 3.11
Message Broker: Redpanda (Kafka-compatible)
Database: SQLite
Monitoring: Prometheus + Grafana
Alerts: Slack Webhooks
Containerization: Docker + Docker Compose
CI/CD: GitHub Actions (setup)
Project Structure
crypto-streaming-pipeline/
├── src/
│   ├── producer.py          # Fetch crypto prices, publish to Redpanda
│   ├── consumer.py          # Consume messages, store in SQLite, send Slack alerts
│   ├── query.py             # Analytics queries: stats, moving averages, hourly agg
│   └── requirements.txt      # Python dependencies
├── docker-compose.yml       # Container orchestration (Redpanda, Prometheus, Grafana)
├── .env                     # Environment variables (git-ignored)
├── .github/workflows/       # GitHub Actions (CI/CD skeleton)
└── README.md               # This file
Quick Start
Prerequisites
Python 3.11+
Docker + Docker Compose
Git
Installation
Clone repository:
bash
git clone https://github.com/YOUR_USERNAME/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
Setup Python environment:
bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r src/requirements.txt
Configure environment:
bash
# Create .env file
cat > .env << EOF
REDPANDA_BROKER=localhost:9092
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=securepassword123
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR_WEBHOOK_URL
EOF
Running

Option 1: Docker Compose (Recommended)

Start all services:

bash
docker-compose up -d

Verify services:

bash
docker ps

Option 2: Local Development (3 terminals)

Terminal 1 — Producer:

bash
cd src
python producer.py

Terminal 2 — Consumer:

bash
cd src
python consumer.py

Terminal 3 — Query (one-time or periodic):

bash
cd src
python query.py
Features
Producer (src/producer.py)
Fetches crypto prices (BTC, ETH, SOL) from CoinGecko API every 10 seconds
Publishes to Redpanda topic crypto-prices
Timezone-aware timestamps (Asia/Jakarta)
Error handling with exponential backoff
Handles rate limiting gracefully (429 errors)
Consumer (src/consumer.py)
Consumes messages from Redpanda topic
Stores in SQLite database (crypto.db) with atomic inserts
Feature 1: Real-time Slack alerts on price changes >5%
Automatic table creation on first run
Consumer group tracking (offset management)
Graceful shutdown (Ctrl+C)
Query Analytics (src/query.py)
Latest prices per symbol
Price statistics: min, max, avg, range, data points
Feature 2: Moving averages (1-hour & 4-hour MA)
Hourly aggregation with trends (last 12 hours)
Formatted output via tabulate
Database Schema

Table: crypto_prices

sql
id (INTEGER PRIMARY KEY AUTOINCREMENT)
timestamp (TEXT)           -- ISO format timestamp
symbol (TEXT)              -- BTC, ETH, SOL
price (REAL)               -- Price in USD
currency (TEXT)            -- Currency (USD)
inserted_at (TIMESTAMP)    -- Auto-inserted at INSERT time
Features in Depth
Feature 1: Real-Time Slack Alerts

Automatically sends Slack notifications when crypto price changes >5%.

Setup:

Create Slack workspace (or use existing): https://slack.com
Create channel: #crypto-alerts
Create Incoming Webhook:
Go to https://api.slack.com/apps
Create New App → Blank App
Enable Incoming Webhooks
Add webhook to #crypto-alerts
Copy webhook URL
Add to .env:
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../X...
Run consumer—alerts trigger automatically:
bash
   python src/consumer.py

Example Slack Alert:

📈 BTC Price Alert
6.10% change detected

Previous: $77,800.00
Current: $82,428.00
2026-09-15 11:05:00

How It Works:

Consumer checks previous price in DB after each insert
Calculates percent change
If >5%: sends formatted Slack message
Uses ✅ emoji for alerts (up/down arrows indicate direction)
Feature 2: Moving Averages

Calculates 1-hour and 4-hour moving averages for technical analysis.

Usage:

bash
python src/query.py

Output Includes:

Latest Prices — Current price for each symbol
Statistics — Min, max, avg, range, data points
Moving Averages:
1-Hour MA: Average price over last 60 minutes
4-Hour MA: Average price over last 240 minutes
Data points: How many prices in each window
Hourly Aggregation — Bucketed stats per hour (last 12 hours)

Example Output:

🔄 MOVING AVERAGES
+----------+-------------+-------------+
| Symbol   |   1-Hour MA |   4-Hour MA |
+==========+=============+=============+
| BTC      |    78071.8  |    78071.8  |
| ETH      |     2503.67 |     2503.67 |
| SOL      |      101.71 |      101.71 |
+----------+-------------+-------------+

Technical Details:

Uses SQL window functions (AVG() with CASE statements)
Filters by timestamp >= NOW() - N hours
Handles data with sparse timestamps gracefully
Useful for trend detection and strategy signals
Monitoring with Prometheus + Grafana
Prometheus
Endpoint: http://localhost:9090
Scrape interval: 15 seconds
Data retention: 15 days
Target: Redpanda metrics at redpanda-broker:9644

Verify targets:

bash
curl http://localhost:9090/api/v1/targets
Grafana
Endpoint: http://localhost:3000
Credentials: admin / securepassword123
Pre-configured: Prometheus data source

Access Dashboard:

Go to http://localhost:3000
Login with credentials above
Dashboards → Redpanda Crypto Pipeline

Dashboard Panels:

Kafka Request Handlers: All request types (produce, fetch, offset_commit, etc.)
Partition Count: Growing partition offsets
Redpanda Uptime: Broker uptime tracking

Available Metrics:

Metric	Description
vectorized_kafka_handler_requests_completed_total	Total Kafka requests by handler
vectorized_storage_log_log_segments_active	Active log segments
vectorized_cluster_partition_end_offset	Partition message count
vectorized_application_uptime	Broker uptime (ms)
process_resident_memory_bytes	Broker memory usage
API Reference
CoinGecko API
Endpoint: https://api.coingecko.com/api/v3/simple/price
Auth: None (public API)
Rate limit: ~50 calls/minute (free tier)
Used by: Producer every 10 seconds
Redpanda
Broker (local): localhost:9092
Broker (Docker): redpanda-broker:9092
Topic: crypto-prices
Consumer Group: crypto-consumer-group
Metrics port: 9644
Prometheus
Query endpoint: http://localhost:9090/api/v1/query
Targets endpoint: http://localhost:9090/api/v1/targets
Slack
Webhook: Set via SLACK_WEBHOOK_URL env var
Rate limit: Depends on Slack plan
Troubleshooting
Producer: "429 Too Many Requests"
Cause: Hit CoinGecko rate limit (~50 calls/min)
Solution: Wait 1-2 minutes, producer auto-reconnects
Long-term: Reduce polling frequency or use paid API tier
Consumer: "Unable to bootstrap from ['localhost:9092']"
Cause: Redpanda not running or not fully started
Solution:
bash
  docker ps  # Verify container running
  docker-compose logs redpanda  # Check logs
  docker-compose restart redpanda
Consumer: "No such file or directory: crypto.db"
Cause: SQLite hasn't created DB yet
Solution: DB creates automatically on first run—just retry
Grafana: "No data" in panels
Cause: Prometheus needs 2-3 scrape cycles (~45 sec) to collect data
Solution: Wait 1-2 minutes, then refresh Grafana
Slack Alert Not Working
Check:
bash
  # Verify webhook in .env
  echo $SLACK_WEBHOOK_URL
  
  # Test webhook manually
  curl -X POST -H 'Content-type: application/json' \
    --data '{"text":"Test"}' \
    $SLACK_WEBHOOK_URL
Fix: Regenerate webhook at https://api.slack.com/apps
Performance Metrics
Metric	Value
Messages/cycle	3 (BTC, ETH, SOL)
Cycle interval	10 seconds
Monthly throughput	~86,400 messages
Producer latency	<100ms
Consumer latency	<50ms
Slack alert latency	<2 seconds
DB insert time	<10ms per message
Storage (1 month)	~50MB SQLite + ~100MB Prometheus
CPU (idle)	<1% producer, <1% consumer, <5% Redpanda
Memory	~50MB + 30MB + 512MB + 512MB (total ~1.1GB)
Project Status
Phase	Component	Status
1-3	Core Pipeline (Producer/Consumer/Query)	✅ Complete
4	Airflow Orchestration	❌ Abandoned (over-engineered)
5	Prometheus + Grafana Monitoring	✅ Complete
6	Feature 1: Slack Price Alerts	✅ Complete
7	Feature 2: Moving Averages	✅ Complete
8	Blog Post / Writeup	⏳ In Progress
Key Design Decisions
Why Redpanda over Kafka?
Drop-in Kafka replacement
Lower resource overhead
Faster to stand up
Sufficient for this scale
Why SQLite?
Zero setup
ACID guarantees
Built into Python
Sufficient for ~100K messages/month
Why Docker Compose?
Simple, reproducible
Works cross-platform
Good for development + monitoring stack
Why Slack Alerts?
Real-time notifications
Integrated workflow
No separate infrastructure
Better than polling
Why Moving Averages?
Technical analysis depth
Shows SQL competency
Useful for trend detection
Portfolio value (data engineering)
Learnings & Insights
1. Windows Development Constraints
Avoid C extension dependencies (confluent-kafka, DuckDB)
Use pure Python libraries (kafka-python, sqlite3)
Docker networking: localhost vs service names matter
2. Over-Engineering Kills Momentum
Airflow added complexity without value
Simple polling + persistence is enough
Ship MVP first, optimize later
3. Observability Matters
Prometheus + Grafana caught issues early
Metrics visualization > debugging logs
Worth setting up from day 1
4. Real-Time Alerts > Polling
Integrated alerts (in consumer) = better UX
No need for separate scheduler
Triggers immediately on price changes
Future Enhancements
 Serverless deployment (AWS Lambda, Google Cloud Functions)
 Time-series DB (InfluxDB, TimescaleDB) for scale
 Kafka Schema Registry for message evolution
 Integration tests for producer/consumer idempotency
 Email/SMS alerts in addition to Slack
 Price predictions (ML model)
 Multi-exchange support (Binance, Kraken)
 Web dashboard (React/Vue frontend)
# Project Gecko: Crypto Streaming Pipeline

A real-time cryptocurrency price streaming and analytics pipeline. Fetches live crypto prices from CoinGecko API, publishes to Redpanda (Kafka-compatible), consumes messages, stores in SQLite, and provides analytics dashboards.

## Architecture

```
CoinGecko API
    ↓
Producer (Python)
    ↓
Redpanda (Message Broker)
    ↓
Consumer (Python)
    ↓
SQLite (Data Storage)
    ↓
Query Analytics (Python)
    ↓
Prometheus/Grafana (Monitoring & Dashboards)
```

## Tech Stack

- **Producer/Consumer/Analytics**: Python 3.11
- **Message Broker**: Redpanda (Kafka-compatible)
- **Database**: SQLite
- **Monitoring**: Prometheus + Grafana
- **Containerization**: Docker + Docker Compose
- **CI/CD**: GitHub Actions (setup)

## Project Structure

```
crypto-streaming-pipeline/
├── src/
│   ├── producer.py          # Fetch crypto prices, publish to Redpanda
│   ├── consumer.py          # Consume messages, store in SQLite
│   ├── query.py             # Analytics queries on SQLite
│   └── requirements.txt      # Python dependencies
├── airflow/                 # (Deprecated—use manual/serverless instead)
├── docker-compose.yml       # Container orchestration
├── .env                     # Environment variables
├── .github/workflows/       # GitHub Actions (setup only)
└── README.md               # This file
```

## Setup

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- Git

### Installation

1. **Clone repository:**
```bash
git clone https://github.com/YOUR_USERNAME/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
```

2. **Setup environment:**
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (macOS/Linux)
source venv/bin/activate

# Install dependencies
pip install -r src/requirements.txt
```

3. **Configure .env:**
```bash
# .env file
REDPANDA_BROKER=localhost:9092
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=securepassword123
```

## Running

### Option 1: Docker Compose (Recommended)

Start all services:
```bash
docker-compose up -d
```

Verify running:
```bash
docker ps
```

Should see: redpanda, prometheus, grafana

### Option 2: Manual (Local Development)

**Terminal 1 — Producer** (fetches prices every 10 sec):
```bash
cd src
python producer.py
```

**Terminal 2 — Consumer** (consume messages, store in SQLite):
```bash
cd src
python consumer.py
```

**Terminal 3 — Query** (analytics, one-time run):
```bash
cd src
python query.py
```

## Features

### Producer (`producer.py`)
- Fetches crypto prices (BTC, ETH, SOL) from CoinGecko API every 10 seconds
- Publishes to Redpanda topic `crypto-prices`
- Timezone-aware (Asia/Jakarta)
- Error handling + retry logic

### Consumer (`consumer.py`)
- Consumes messages from Redpanda
- Stores in SQLite database (`crypto.db`)
- Automatic table creation
- Graceful shutdown (Ctrl+C)

### Query Analytics (`query.py`)
- Latest prices per symbol
- Price statistics (avg, min, max, range)
- Hourly aggregation
- Formatted table output (via `tabulate`)

## Database Schema

**Table: `crypto_prices`**
```sql
id (INTEGER PRIMARY KEY)
timestamp (TEXT)
symbol (TEXT)
price (REAL)
currency (TEXT)
inserted_at (TIMESTAMP)
```

## Monitoring

### Prometheus
- Endpoint: `http://localhost:9090`
- Scrapes Redpanda metrics
- Data retention: 15 days (default)

### Grafana
- Endpoint: `http://localhost:3000`
- Default credentials: `admin` / `securepassword123`
- Dashboards for Redpanda metrics (setup required)

## API Reference

### CoinGecko API
- Endpoint: `https://api.coingecko.com/api/v3/simple/price`
- Params: `ids`, `vs_currencies`
- No authentication required
- Rate limit: ~50 calls/min

### Redpanda
- Broker: `localhost:9092` (local) or `redpanda-broker:9092` (Docker)
- Topic: `crypto-prices`
- Consumer Group: `crypto-consumer-group`

## Troubleshooting

### Producer: "Unable to bootstrap from ['localhost:9092']"
- Ensure Docker containers running: `docker ps`
- Redpanda takes 30s to fully start after container creation
- Verify broker accessibility: `docker exec redpanda-broker rpk cluster info`

### Consumer: "No such file or directory: crypto.db"
- SQLite creates DB automatically on first run
- Ensure write permissions in working directory

### Query: "Table 'crypto_prices' doesn't exist"
- Run consumer first to create table
- Check database: `python -m sqlite3 crypto.db ".tables"`

## Performance

- **Data throughput**: ~3 messages/10sec (1 cycle = BTC, ETH, SOL)
- **Database size**: ~1KB per cycle (~86MB/month at 10min intervals)
- **CPU**: Minimal (producer: <1%, consumer: <1%)
- **Memory**: ~50MB (producer) + 30MB (consumer)

## Future Enhancements

- [ ] Advanced analytics (moving averages, volatility)
- [ ] Email/Slack alerts on price thresholds
- [ ] Web dashboard (React/Vue)
- [ ] Multiple exchanges (Binance, Kraken)
- [ ] ML predictions
- [ ] Cloud deployment (AWS, GCP, Azure)

## License

MIT

## Author

Jeremy Krisna

---

**Status**: MVP Complete (FASE 1-6)
- ✓ Producer: Publishing crypto prices
- ✓ Consumer: Consuming & storing data
- ✓ Query: Analytics working
- ✓ Docker: Containerized
- ✓ Monitoring: Prometheus/Grafana ready
- ✓ GitHub: Repo setup (CI/CD pending production deployment)
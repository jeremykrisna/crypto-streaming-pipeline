# Real-Time Crypto Streaming Pipeline

A production-grade, end-to-end data streaming pipeline that ingests cryptocurrency prices in real-time, demonstrates Kafka-style message brokering, and provides observability through modern monitoring tools.

## Problem Statement

Building a streaming data infrastructure from scratch is non-trivial. Most tutorials either oversimplify (single-threaded scripts) or assume prior experience with distributed systems. This project bridges that gap by implementing a **real, working pipeline** that handles:

- **Real-time data ingestion** from a public API
- **Reliable message brokering** with Kafka-compatible broker
- **Persistent storage** with atomic inserts
- **Monitoring & observability** from day one
- **Graceful error handling** and retry logic

The goal: demonstrate full-stack data engineering skills with a portfolio-ready project.

## Solution Architecture

```
┌─────────────────┐
│  CoinGecko API  │  (Live crypto prices)
└────────┬────────┘
         │
    ┌────▼──────────────────────────────────┐
    │  Producer (Python)                    │
    │  • 10-sec polling                     │
    │  • SSLContext bypass (corp proxy)     │
    │  • Timezone-aware timestamps          │
    └────────┬─────────────────────────────┘
             │
    ┌────────▼──────────────────────────────┐
    │  Redpanda Broker                      │
    │  • Kafka-compatible                   │
    │  • Topic: crypto-prices               │
    │  • Metrics endpoint: :9644            │
    └────────┬──────────────────────────────┘
             │
    ┌────────▼──────────────────────────────┐
    │  Consumer (Python)                    │
    │  • Group-based consumption            │
    │  • SQLite atomic inserts              │
    │  • Graceful shutdown (Ctrl+C)         │
    └────────┬──────────────────────────────┘
             │
    ┌────────▼──────────────────────────────┐
    │  SQLite Database (crypto.db)          │
    │  • Schema: id, timestamp, symbol,     │
    │    price, currency, inserted_at       │
    └────────┬──────────────────────────────┘
             │
    ┌────────▼──────────────────────────────┐
    │  Monitoring Stack                     │
    │  • Prometheus (metrics scraping)      │
    │  • Grafana (dashboards)               │
    │  • Real-time Redpanda observability   │
    └───────────────────────────────────────┘
```

## Key Design Decisions

### 1. Redpanda over Kafka
- **Why**: Drop-in Kafka replacement, lower resource footprint, faster to stand up
- **Trade-off**: Less mature ecosystem than Kafka, but sufficient for this scale

### 2. SQLite for Persistence
- **Why**: Zero setup, ACID guarantees, sufficient for ~100K messages/month
- **Pain point discovered**: SQLite locks with Airflow SequentialExecutor (see Learnings)
- **Alternative rejected**: DuckDB (wheel compilation failed on Windows)

### 3. Docker Compose for Orchestration
- **Why**: Simple, reproducible, works cross-platform
- **Rejected Airflow**: Too complex for this use case (see Airflow Complexity section)

### 4. Prometheus + Grafana
- **Why**: Industry standard observability, built-in Redpanda metrics, zero-config Grafana dashboards
- **Learned**: Grafana's label filtering has parsing quirks on some versions

## Technical Highlights

### Producer (`src/producer.py`)
- Fetches BTC/ETH/SOL prices every 10 seconds from CoinGecko
- Custom SSL context for corporate proxy environments
- Timezone-aware timestamps (Asia/Jakarta)
- Exponential backoff on API failures
- Publishes 3 messages/cycle (~86K messages/month)

### Consumer (`src/consumer.py`)
- Subscribes to `crypto-consumer-group`
- Atomic inserts to SQLite with `PRAGMA journal_mode = WAL`
- Tracks `committed_offset` per cycle
- Handles graceful shutdown without message loss

### Query Analytics (`src/query.py`)
- Aggregates price statistics (latest, avg, min, max)
- Hourly bucketing (note: SQLite TEXT timestamp limitation)
- Formatted table output via `tabulate`

### Monitoring
- Prometheus scrapes Redpanda metrics every 15 seconds
- Grafana dashboard tracks:
  - Kafka request handlers (produce/fetch/offset_commit)
  - Active log segments
  - Partition count growth
  - Broker uptime

## What Went Well ✅

| Challenge | Outcome |
|-----------|---------|
| **Windows Docker networking** | Solved: Use `localhost:9092` locally, `redpanda-broker:9092` inside containers |
| **Python dependency compilation** | Solved: Switched from `confluent-kafka` → `kafka-python` (pure Python, no C ext) |
| **Real-time pipeline debugging** | Solved: `rpk topic consume` directly from container |
| **End-to-end data validation** | Solved: Query validates message count matches DB inserts |
| **Observability from day 1** | Win: Prometheus + Grafana running before core code finished |

## What Failed & Why ❌

### Airflow Orchestration (ABANDONED)
**Attempted**: Use Apache Airflow to schedule producer/consumer DAGs

**Problems encountered**:
1. **SQLite locking**: Airflow SequentialExecutor (no Postgres) causes database locks
2. **Volume mounting conflicts**: `airflow_data:/opt/airflow` volume overrides bind mounts
3. **Dependency hell**: Python package versions (Flask, Werkzeug) conflicted
4. **Windows-specific**: WSL2 networking added another layer of complexity

**Decision**: Abandoned for this project scope. Producer/consumer run as standalone services instead.

**Lesson**: Orchestration frameworks have high complexity overhead. For simple polling pipelines, Kubernetes CronJobs or serverless functions are lighter-weight.

### DuckDB (REJECTED)
**Attempted**: Use DuckDB instead of SQLite for better analytics

**Problem**: Wheel build failed on Windows. No pre-built binary available.

**Decision**: Fell back to SQLite (already built into Python).

**Lesson**: Always vet Windows binary availability before adding dependencies. Linux-first projects often fail on Windows.

## Learnings & Principles

### 1. Windows Compilation Constraints
- Avoid C extension dependencies (`confluent-kafka`, `DuckDB`)
- Stick with pure Python libraries (`kafka-python`, `sqlite3`)
- Test on Windows early, not as an afterthought

### 2. Docker Networking Clarity
- Containers see each other via service names: `redpanda-broker:9092`
- Host machine sees containers via `localhost:9092`
- Mix them up → hours of debugging

### 3. Scope Discipline
- Don't add features speculatively (Airflow wasn't needed)
- Complete MVP first, optimize later
- Each phase should be shippable independently

### 4. Observability > Debugging
- Prometheus + Grafana running early provided visibility into broker health
- Saved time troubleshooting consumer lag
- "If you can't see it, you can't fix it"

### 5. Documentation as Design
- Writing setup instructions exposed configuration complexity (Airflow)
- Simpler architecture = simpler docs

## Performance Metrics

| Metric | Value |
|--------|-------|
| Messages/cycle | 3 (BTC, ETH, SOL) |
| Cycle interval | 10 seconds |
| Monthly throughput | ~86,400 messages |
| Producer latency | <100ms |
| Consumer latency | <50ms |
| DB insert time | <10ms per message |
| Storage (1 month) | ~50MB SQLite + ~100MB Prometheus |
| CPU (idle) | <1% (producer/consumer), <5% (Redpanda) |
| Memory (running) | 50MB + 30MB + 512MB + 512MB (total ~1GB) |

## Tech Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Language** | Python 3.11 | Rapid development, strong data libs |
| **Message Broker** | Redpanda v24.1.1 | Kafka-compatible, low overhead |
| **Database** | SQLite | Zero setup, ACID, sufficient scale |
| **Monitoring** | Prometheus + Grafana | Industry standard, built-in Redpanda metrics |
| **Containerization** | Docker Compose | Simple, reproducible, cross-platform |
| **Development** | VS Code, PowerShell, Git | Windows-native workflow |

## How to Run (Quick Start)

```bash
# Clone and setup
git clone https://github.com/YOUR_USERNAME/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r src/requirements.txt

# Start infrastructure
docker-compose up -d

# Run pipeline (3 terminals)
python src/producer.py
python src/consumer.py
python src/query.py

# Monitor
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin/securepassword123)
```

For detailed setup, see [README_SETUP.md](./README_SETUP.md).

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1-3 | Producer → Redpanda → Consumer → SQLite | ✅ Complete |
| 4 | Airflow DAGs | ❌ Abandoned (over-engineered) |
| 5 | Prometheus + Grafana monitoring | ✅ Complete |
| 6 | GitHub Actions CI/CD | ⏳ Infrastructure ready |
| 7 | Cloud deployment (VPS/Serverless) | 🔲 Planned |

## Lessons for Future Work

1. **Serverless functions** (AWS Lambda, Google Cloud Functions) instead of VPS for scheduled tasks
2. **Time-series DB** (InfluxDB, TimescaleDB) instead of SQLite for analytics scale
3. **Kafka Schema Registry** for message schema evolution
4. **Integration tests** for producer/consumer idempotency
5. **Alerting** (PagerDuty, Opsgenie) on pipeline failures

## Repository Structure

```
crypto-streaming-pipeline/
├── src/
│   ├── producer.py              # ~80 lines
│   ├── consumer.py              # ~90 lines
│   ├── query.py                 # ~60 lines
│   └── requirements.txt
├── docker-compose.yml           # Redpanda, Prometheus, Grafana
├── .env                         # Secrets (git-ignored)
├── .github/workflows/           # CI/CD skeleton
├── README.md                    # This file (portfolio summary)
├── README_SETUP.md              # Detailed setup instructions
└── crypto.db                    # SQLite (git-ignored)
```

## Conclusion

Project Gecko demonstrates:
- ✅ End-to-end data pipeline design
- ✅ Real-world troubleshooting (Windows Docker, dependency conflicts)
- ✅ Observability architecture (Prometheus/Grafana)
- ✅ Code quality (error handling, graceful shutdown)
- ✅ Documentation and communication

This isn't a toy project—it's a **working, production-inspired system** that handles real data, demonstrates maturity in decision-making, and shows I can see something through from conception to completion.

---

**Want to see it in action?** Clone the repo and run `docker-compose up -d`. In 30 seconds you'll have a live streaming pipeline with real-time monitoring dashboards.
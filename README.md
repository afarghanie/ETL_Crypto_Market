#  Crypto Market ETL Pipeline

A production-ready data engineering pipeline that extracts cryptocurrency market data, transforms it using Apache Spark, and loads it into PostgreSQL for visualization in Metabase.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

##  Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for cryptocurrency market data using modern data engineering practices.

### Pipeline Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   CoinGecko     │────▶│     MinIO       │────▶│   Apache        │────▶│   PostgreSQL    │────▶│    Metabase     │
│   API           │     │   (Raw JSON)    │     │   Spark         │     │   (Warehouse)   │     │   (Dashboard)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │                       │                       │
     Extract              Store Raw              Transform              Load Clean             Visualize
```

### Data Flow

1. **Check API Availability** - Sensor validates CoinGecko API is responsive
2. **Extract** - Fetch top 10 cryptocurrencies by market cap
3. **Store Raw** - Save JSON data to MinIO object storage
4. **Transform** - Spark job flattens nested structures, adds timestamps
5. **Load** - Insert cleaned data into PostgreSQL
6. **Visualize** - Metabase dashboards for analysis

##  Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | Apache Airflow | Workflow scheduling & monitoring |
| **Data Source** | CoinGecko API | Real-time crypto market data |
| **Object Storage** | MinIO | S3-compatible raw data lake |
| **Processing** | Apache Spark | Distributed data transformation |
| **Database** | PostgreSQL | Analytical data warehouse |
| **Visualization** | Metabase | BI dashboards & reporting |
| **Infrastructure** | Docker Compose | Container orchestration |
| **CLI** | Astronomer CLI | Airflow deployment & management |

##  Data Collected

The pipeline captures the following metrics for each cryptocurrency:

- **Identity**: `coin_id`, `symbol`, `name`
- **Price**: `price_usd`, `high_24h`, `low_24h`
- **Market**: `market_cap`, `volume_24h`
- **Changes**: `price_change_24h`, `price_change_pct_24h`
- **Timestamps**: `last_updated`, `extracted_at`

##  Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (8GB+ RAM recommended)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli/)

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/ETL_Crypto_Market.git
cd ETL_Crypto_Market

# Start the entire stack
astro dev start
```

### Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | `minio` / `minio123` |
| **Metabase** | http://localhost:3000 | Setup on first visit |
| **Spark Master** | http://localhost:8082 | - |
| **pgAdmin** | http://localhost:5050 | `admin@example.com` / `admin` |

## Project Structure

```
.
├── dags/
│   └── crypto_market.py     # Main ETL DAG with TaskFlow API
├── spark/
│   ├── master/              # Spark master Dockerfile
│   ├── worker/              # Spark worker Dockerfile
│   └── notebooks/
│       └── stock_transform/
│           └── crypto_transform.py  # PySpark transformation script
├── include/
│   ├── data/                # Persistent data volumes
│   └── sql/
│       └── schema.sql       # PostgreSQL schema definition
├── docker-compose.override.yml  # Additional services (MinIO, Spark, Metabase)
├── airflow_settings.yaml    # Airflow connections & variables
├── requirements.txt         # Python dependencies
└── README.md
```

##  Configuration

### Airflow Connections

The pipeline uses these preconfigured connections:

| Connection ID | Service | Description |
|---------------|---------|-------------|
| `coingecko_api` | CoinGecko | REST API endpoint |
| `minio` | MinIO | S3-compatible storage |
| `postgres_default` | PostgreSQL | Data warehouse |


##  Pipeline Details

### DAG Schedule

The pipeline runs daily at midnight UTC (`0 0 * * *`).

### Airflow Tasks

```python
check_api_availability >> fetch_top3_blockchain >> store_data >> transform_data >> load_data
```

| Task | Type | Description |
|------|------|-------------|
| `check_api_availability` | Sensor | Polls API until ready |
| `fetch_top3_blockchain` | Task | Fetches top 10 coins |
| `store_data` | Task | Saves JSON to MinIO |
| `transform_data` | DockerOperator | Runs Spark job in container |
| `load_data` | Task | Loads CSV into PostgreSQL |

##  Development

### Restart Services

```bash
# Stop all containers
astro dev stop

# Kill and restart (useful for Spark issues)
astro dev kill && astro dev start
```

### View Logs

```bash
# Airflow scheduler logs
astro dev logs scheduler

# All service logs
astro dev logs
```

##  Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

##  License

This project is open source and available under the [MIT License](LICENSE).

##  Acknowledgments

- [CoinGecko](https://www.coingecko.com/) for the free cryptocurrency API
- [Astronomer](https://www.astronomer.io/) for the Airflow managed platform
- [Apache Airflow](https://airflow.apache.org/) community

---

 **Alavie I. Farghanie - Data Engineer Enthusiast**

# E-Commerce Data Pipeline Project

**Student Name:** Bharghav Sai Marla  
**Roll Number:** 23P31A0503  

---

## 📌 Project Overview
This project implements an end-to-end **data engineering pipeline** for an e-commerce analytics platform.  
It covers the complete lifecycle from **data generation → ingestion → transformation → warehousing → analytics → BI dashboards**, following industry best practices.

---

## 🛠️ Prerequisites

Ensure the following tools are installed before setup:

- **Python 3.8+**
- **PostgreSQL 12+**
- **Docker & Docker Compose**
- **Git**
- **Tableau Public OR Power BI Desktop (Free version)**

---

## ⚙️ Installation Steps

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/marlabharghavsai/ecommerce-data-pipeline-23P31A0503.git
cd ecommerce-data-pipeline-23P31A0503
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker-compose up -d

```

## Project Architecture

This project implements a complete end-to-end **data analytics pipeline** for an e-commerce platform, transforming raw transactional data into business insights through a dimensional data warehouse and BI dashboards.

### Data Flow Overview

```
Raw CSV Data
↓
Staging Schema (PostgreSQL)
↓
Production Schema (Cleaned & Validated)
↓
Warehouse Schema (Star Schema)
↓
Analytics Queries
↓
BI Dashboard (Tableau / Power BI)

```

## Technology Stack

- **Data Generation:** Python (Faker)
- **Database:** PostgreSQL 14
- **ETL & Transformations:** Python (psycopg2, pandas)
- **Data Modeling:** Kimball Star Schema
- **Orchestration:** Custom Python Pipeline Orchestrator
- **Scheduling:** Python Scheduler
- **Monitoring & Alerting:** Python Monitoring Scripts
- **Containerization:** Docker & Docker Compose
- **Testing:** Pytest with Coverage
- **BI Visualization:** Tableau Public / Power BI Desktop

---

## Project Structure

```
ecommerce-data-pipeline/
├── config/
│ └── config.yaml
├── data/
│ ├── raw/
│ ├── staging/
│ ├── production/
│ └── processed/
├── dashboards/
│ ├── screenshots/
│ └── tableau/
├── docs/
│ ├── architecture.md
│ └── dashboard_guide.md
├── logs/
├── scripts/
│ ├── data_generation/
│ ├── ingestion/
│ ├── transformation/
│ ├── monitoring/
│ ├── pipeline_orchestrator.py
│ ├── scheduler.py
│ └── cleanup_old_data.py
├── sql/
│ ├── ddl/
│ └── queries/
├── tests/
├── docker-compose.yml
├── pytest.ini
└── README.md

```

## Running the Pipeline
- Full Pipeline Execution : python scripts/pipeline_orchestrator.py
  
### Individual Steps
- python scripts/data_generation/generate_data.py
- python scripts/ingestion/ingest_to_staging.py
- python scripts/quality_checks/validate_data.py
- python scripts/transformation/staging_to_production.py
- python scripts/transformation/load_warehouse.py
- python scripts/transformation/generate_analytics.py

### Running Tests
- pytest tests/ -v
- With coverage: pytest --cov=scripts --cov-report=term-missing
 
### Dashboard Access
- Tableau Public URL: https://public.tableau.com/app/profile/bharghav.sai.marla/vizzes
- Screenshots: dashboards/screenshots/

### Database Schemas
- Staging Schema
  - staging.customers
  - staging.products
  - staging.transactions
  - staging.transaction_items
- Production Schema
  - production.customers
  - production.products
  - production.transactions
  - production.transaction_items
- Warehouse Schema
  - warehouse.dim_customers
  - warehouse.dim_products
  - warehouse.dim_date
  - warehouse.dim_payment_method
  - warehouse.fact_sales
  - warehouse.agg_daily_sales
  - warehouse.agg_product_performance
  - warehouse.agg_customer_metrics

### Key Insights from Analytics
- Electronics category generates the highest revenue
- Strong revenue growth observed during Q4
- VIP customers contribute a disproportionate share of revenue
- Weekend sales outperform weekdays
- Online payment methods dominate transactions

### Challenges & Solutions

1.Duplicate warehouse loads
  - Solved using TRUNCATE + idempotent inserts
2.Data quality mismatches
  - Implemented reconciliation logic
3.Scheduler blocking tests
  - Used __main__ guards and test isolation
4.Timezone inconsistencies
  - Standardized all timestamps to UTC
5.Coverage failures
  - Added focused unit tests and import validation


### Future Enhancements
- Real-time streaming with Apache Kafka
- Cloud deployment on AWS/GCP
- Machine learning for demand forecasting
- Real-time alerting with Slack or Email
- Incremental warehouse loading

### Contact

- Name: Bharghav Sai
- Roll Number: 23P31A0503
- Email: marlabharghavsai@gmail.com




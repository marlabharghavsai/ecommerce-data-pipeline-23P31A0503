# API & SCRIPT DOCUMENTATION  
E-Commerce Data Pipeline

---

## Overview

This document describes all **scripts, modules, and interfaces** used in the E-Commerce Data Pipeline project.  
Although this project does not expose a REST API, it provides **script-based APIs** that act as standardized interfaces between pipeline stages.

Each script is designed to be:
- Deterministic
- Idempotent
- Independently executable
- Orchestrated by a central pipeline controller

---

## Pipeline Script Interfaces

The pipeline consists of multiple Python scripts, each acting as a functional API.

---

## 1. Data Generation API

### Script
``` scripts/data_generation/generate_data.py ```

### Purpose
Generates synthetic e-commerce data using Faker.

### Inputs
- Configuration from `config/config.yaml`
- Parameters:
  - Number of customers
  - Number of products
  - Number of transactions
  - Date range

### Outputs
CSV files written to:
```
data/raw/
├── customers.csv
├── products.csv
├── transactions.csv
├── transaction_items.csv

```
### Guarantees
- Referential integrity preserved
- Deterministic with fixed seed
- No NULLs in mandatory fields

### Invocation
```bash
python scripts/data_generation/generate_data.py
```

## Ingestion API
- Script: scripts/ingestion/ingest_to_staging.py

### Purpose
- Loads raw CSV data into PostgreSQL staging schema.

### Inputs
- CSV files from data/raw/
- Database credentials from config.yaml
#### Outputs
- Tables populated:
  - staging.customers
  - staging.products
  - staging.transactions
  - staging.transaction_items

### Behavior
- TRUNCATE before load
- Batch insert
- Transactional (rollback on failure)

### Invocation
```python scripts/ingestion/ingest_to_staging.py ```

## Data Quality Checks API
### Script
``` scripts/quality_checks/validate_data.py ```

### Purpose
- Validates staging data before transformation.

### Checks Performed
- NULL checks
- Duplicate detection
- Referential integrity
- Calculation consistency
- Range validations
#### Output
``` data/quality/data_quality_report.json ```

### Invocation
``` python scripts/quality_checks/validate_data.py ```

## Staging → Production ETL API
### Script
``` scripts/transformation/staging_to_production.py ```

### Purpose
- Cleans, validates, and loads data into production schema.
#### Key Operations
- Text normalization
- Email standardization
- Profit margin calculation
- Price category assignment
- Transaction total reconciliation
#### Load Strategy
- Dimensions: Full truncate & reload
- Facts: Incremental append-only
#### Outputs
  - production.customers
  - production.products
  - production.transactions
  - production.transaction_items

### Summary Output
``` data/production/transformation_summary.json ```

### Invocation
``` python scripts/transformation/staging_to_production.py ```

## Warehouse Load API
### Script
``` scripts/transformation/load_warehouse.py ```

### Purpose
- Builds the dimensional star schema.

### Warehouse Objects Created
- Dimensions:
  - dim_customers (SCD Type 2)
  - dim_products (SCD Type 2)
  - dim_date
  - dim_payment_method
- Fact:
  - fact_sales
- Aggregates:
  - agg_daily_sales
  - agg_product_performance
  - agg_customer_metrics

### Behavior
- Idempotent inserts
- Surrogate key lookups
- FK integrity enforced

### Invocation
``` python scripts/transformation/load_warehouse.py ```

## Analytics Generation API
### Script
``` scripts/transformation/generate_analytics.py ```

### Purpose
- Executes analytical SQL queries and exports results for BI tools.
#### Outputs
```
data/processed/analytics/
├── query1_top_products.csv
├── query2_monthly_trend.csv
├── ...
├── analytics_summary.json

```

### Invocation
``` python scripts/transformation/generate_analytics.py ```

##  Pipeline Orchestrator API
### Script
``` scripts/pipeline_orchestrator.py ```

### Purpose
- Runs the entire pipeline end-to-end with dependency control.

### Execution Order
- Ingestion
- Quality Checks
- Production ETL
- Warehouse Load
- Analytics

### Features
- Retry logic (exponential backoff)
- Step-level failure isolation
- Centralized logging
- Execution report generation
#### Output 
``` data/processed/pipeline_execution_report.json ```

### Invocation
``` python scripts/pipeline_orchestrator.py``` 

## Scheduler API
### Script
``` scripts/scheduler.py```

### Purpose
- Automates daily pipeline execution.

### Features
- Configurable run time
- Lock file to prevent concurrent runs
- Automatic cleanup execution
- Persistent logging

### Invocation
``` python scripts/scheduler.py ```

## Cleanup API
### Script
``` scripts/cleanup_old_data.py```

### Purpose
- Applies data retention policies.

### Deletes
- Old raw data
- Old staging files
- Old logs

### Preserves
- Summary files
- Reports
- Current-day data

### Configuration
- Retention period defined in config.yaml.

##. Monitoring API
### Script
``` scripts/monitoring/pipeline_monitor.py ```

### Purpose
- Monitors pipeline health, data freshness, anomalies, and database status.

###Checks
- Pipeline execution recency
- Data freshness lag
- Volume anomalies
- Data quality score
- Database connectivity
#### Output
``` data/processed/monitoring_report.json ```

### Invocation
``` python scripts/monitoring/pipeline_monitor.py ```

## Error Handling Strategy
- All scripts raise explicit exceptions
- Orchestrator halts on failure
- Scheduler logs failures without crashing
- Monitoring flags degraded or critical states

## Security Considerations
- Credentials stored in config file (not hardcoded)
- No secrets committed to GitHub
- Local execution only (no exposed endpoints)

## Versioning
- Python: 3.12
- PostgreSQL: 14+
- Pytest: 8+
- Pandas: Latest stable

## Maintainer
- Name: M.Bharghav Sai
- Project: E-Commerce Data Pipeline

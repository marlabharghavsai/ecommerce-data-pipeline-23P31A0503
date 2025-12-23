# E-Commerce Data Pipeline Architecture

## Overview

This document describes the architecture of the e-commerce data analytics platform, designed to ingest, process, store, analyze, and visualize transactional data.

---

## System Components

### 1. Data Generation Layer
- Generates synthetic e-commerce data using Python Faker
- Outputs CSV files: customers, products, transactions, transaction_items

### 2. Data Ingestion Layer
- Loads CSV files into PostgreSQL staging schema
- Uses batch ingestion with transactional safety

### 3. Data Storage Layer

#### Staging Schema
- Raw data replica
- Minimal validation
- Temporary storage

#### Production Schema
- Cleaned and validated data
- 3NF normalization
- Enforced referential integrity

#### Warehouse Schema
- Kimball star schema
- Optimized for analytics
- SCD Type 2 for dimensions

---

## Data Processing Layer

- Data quality checks
- Business rule enforcement
- Dimensional modeling
- Aggregate table generation

---

## Data Serving Layer

- Analytical SQL queries
- Pre-computed aggregates
- CSV exports for BI tools

---

## Visualization Layer

- Tableau Public / Power BI
- Interactive dashboards
- Filters, drill-downs, KPIs

---

## Orchestration Layer

- Pipeline orchestrator
- Scheduler for daily runs
- Monitoring & alerting
- Cleanup & retention policies

---

## Data Models

### Warehouse Star Schema
- Dimensions: Customers, Products, Date, Payment Method
- Fact: Sales
- Aggregates: Daily, Product, Customer metrics

---

## Deployment Architecture

- Dockerized PostgreSQL
- Python ETL services
- Local or cloud-ready deployment

---

## Technology Versions

- Python 3.12
- PostgreSQL 14
- Docker 24.x
- Pytest 8.x

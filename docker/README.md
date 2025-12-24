## Docker Deployment Guide

### E-Commerce Data Pipeline
- This document explains how to deploy, run, and manage the e-commerce data pipeline using Docker and Docker Compose.

### 1. Prerequisites
- Before starting, ensure your system meets the following requirements.
#### Software Requirements
- Docker: >= 20.10
- Docker Compose: >= 2.0 (Docker Compose V2 plugin)
#### Verify installation:
```
docker --version
docker compose version
```
#### System Requirements
- RAM: Minimum 4 GB (8 GB recommended)
- Disk Space: At least 5 GB free
- OS: Linux / macOS / Windows (WSL2 recommended on Windows)

### 2. Quick Start Guide
#### 2.1 Build Docker Images
- From the project root:
```
docker compose build
```
#### 2.2 Start Services
- Start PostgreSQL and the pipeline services:
``` 
docker compose up -d
```
##### This will:
- Start PostgreSQL with persistent storage
- Wait for database health check
- Start the data pipeline container

#### 2.3 Verify Services Are Running
##### Check container status:
```
docker compose ps
```

##### Expected output:
- postgres → healthy
- pipeline → running

#### 2.4 Run the Pipeline in Containers
- To execute the full pipeline manually inside the container:
```
docker compose exec pipeline python scripts/pipeline_orchestrator.py
```
- Or run steps individually:
```
docker compose exec pipeline python scripts/data_generation/generate_data.py
docker compose exec pipeline python scripts/ingestion/ingest_to_staging.py
docker compose exec pipeline python scripts/transformation/staging_to_production.py
docker compose exec pipeline python scripts/transformation/load_warehouse.py
```
#### 2.5 Access the PostgreSQL Database
- Connect to PostgreSQL:
```
docker compose exec postgres psql -U admin -d ecommerce_db
```
- Example query:
```
SELECT COUNT(*) FROM warehouse.fact_sales;
```
#### 2.6 View Logs
- View all service logs:
```
docker compose logs -f
```
- View a specific service:
```
docker compose logs -f postgres
docker compose logs -f pipeline
```
#### 2.7 Stop Services
- Stop all containers:
```
docker compose down
```

#### 2.8 Clean Up (Full Reset)
- This removes containers, volumes, and data.
```
docker compose down -v
docker system prune -f
```
### 3. Configuration
#### 3.1 Environment Variables
- Configured via docker-compose.yml:\
- Variable	Description
```
DB_HOST	PostgreSQL service name (postgres)
DB_PORT	Database port (5432)
DB_NAME	Database name
DB_USER	Database user
DB_PASSWORD	Database password
```

#### 3.2 Volume Mounts
- Persistent volumes ensure data survives restarts:
- PostgreSQL data
- volumes:
``` 
postgres_data:/var/lib/postgresql/data
```
- Pipeline outputs (optional)
- volumes:
```
./data:/app/data
```

#### 3.3 Network Configuration
- Uses Docker Compose default bridge network
- Services communicate using service names
  - Pipeline connects to database via postgres, not localhost

#### 3.4 Resource Limits (Optional)
- Example:
```
deploy:
  resources:
    limits:
      cpus: "1.0"
      memory: 1G
```

### 4. Data Persistence Verification
- To verify persistence:
- Start services:
```
docker compose up -d
```

- Insert data via pipeline
- Stop services:
```
docker compose down
```

- Restart:
```
docker compose up -d
```
- Data remains intact
- Warehouse tables persist
- Volumes prevent data loss

### 5. Troubleshooting
#### 5.1 Port Already in Use
- Error: bind: address already in use
- Fix:
```
sudo lsof -i :5432
sudo kill -9 <PID>
```
- Or change port mapping in docker-compose.yml.

#### 5.2 Database Not Ready
- Symptom: Pipeline fails to connect
- Solution:
  - PostgreSQL health check is enabled
  - Pipeline waits using depends_on with condition: service_healthy

#### 5.3 Permission Issues on Volumes
- Fix:
```
sudo chown -R $USER:$USER .
```
#### 5.4 Container Fails to Start
- Inspect logs:
```
docker compose logs pipeline
docker compose logs postgres
```

#### 5.5 Network Connectivity Issues
- Ensure:
  - DB_HOST=postgres
  - Services are on the same Docker network

### 6. Summary

- Services are isolated
- PostgreSQL uses persistent volumes
- Pipeline waits for DB readiness
- Containers communicate via service names
- Docker setup follows best practices

# Project Submission

## Student Information
- Name: Bharghav Sai Marla
- Roll Number: 23P31A0503
- Email: marlabharghavsai@gmail.com
- Submission Date: 24-12-2025

## GitHub Repository
- Repository URL:https://github.com/marlabhargavsai/ecommerce-data-pipeline-23P31A0503
- Repository Status: Public
- Commit Count: 30+ commits

## Project Completion Status
### Phase 1: Setup (8 points)
- Repository structure created
- Environment setup documented
- Dependencies configured
- Docker configuration completed

### Phase 2: Data Generation & Ingestion (18 points)
- Data generation script implemented
- Database schemas created (staging, production, warehouse)
- Data ingestion completed (raw → staging)

### Phase 3: Transformation & Processing (22 points)
- Data quality checks implemented
- Staging → production ETL completed
- Data warehouse design completed (facts, dimensions, aggregates)

### Phase 4: Analytics & BI (18 points)
- Analytical SQL queries written
- BI dashboard created
- Dashboards available under dashboards/

### Phase 5: Automation (14 points)
- Pipeline orchestrator implemented
- Scheduling configured
- Monitoring implemented

### Phase 6: Testing & Documentation (12 points)
- Unit tests written
- Test coverage enforced via CI
- Documentation completed (README, API docs, Docker docs)

### Phase 7: Deployment (8 points)
- CI/CD pipeline configured using GitHub Actions
- Docker deployment verified
- Final submission prepared

## Dashboard Links
- Tableau Public: Not used
- Power BI Screenshots: dashboards/screenshots/

## Key Deliverables
- Complete source code in GitHub
- SQL scripts for all schemas
- Python scripts for the entire pipeline
- BI dashboards (screenshots provided)
- Unit tests with enforced coverage
- Comprehensive documentation

## Running Instructions
### Clone Repository
```
git clone https://github.com/marlabhargavsa i/ecommerce-data-pipeline-23P31A0503
cd ecommerce-data-pipeline-23P31A0503
```
### Setup Environment
```
bash setup.sh
```
### Run Pipeline
```
python scripts/pipeline_orchestrator.py
```
### Run Tests
```
pytest tests/ -v
```

## Project Statistics
- Total Lines of Code: ~3,500+
- Total Data Records Generated: 30,000+
- Dashboard Visualizations: 16+
- Test Coverage: 100% (coverage enforced via CI)

## Challenges Faced
- 1. PostgreSQL Authentication in CI
  - Issue: GitHub Actions failed due to password prompts
  - Solution: Used PGPASSWORD environment variable and service health checks

- 2. Schema & ETL Mismatch
  - Issue: ETL scripts referenced columns not present in schema
  - Solution: Aligned SQL schemas strictly with transformation logic

- 3. Warehouse FK Truncation Errors
  - Issue: Truncation failed due to foreign key dependencies
  - Solution: Used TRUNCATE ... CASCADE and proper load order

## Declaration
- I hereby declare that this project is my original work and has been completed independently.

## Final Submission Steps
```
git add .
git commit -m "Final submission"
git push origin main
```
- Release Tag
```
git tag -a v1.0 -m "Final Submission"
git push origin v1.0
```









## Signature: Bharghav Sai Marla
## Date: 24-12-2025 

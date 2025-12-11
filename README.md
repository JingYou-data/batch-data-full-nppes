# NPPES Cardiology Data Pipeline

Complete ETL pipeline for processing NPPES (National Provider Identifier) cardiology specialist data.

## Project Overview
- **Data Source**: NPPES registry (8.8M+ providers)
- **Focus**: Cardiology specialists analysis
- **Tech Stack**: DuckDB, MinIO, PostgreSQL, AWS S3, Lambda

## 28-Step Review Checklist

### Environment Setup
- [x] 1. Virtual Environment
- [x] 2. Folder structure (src/, data/, logs/, docker/)
- [x] 3. Environment variables (.env)
- [ ] 4. API data retrieval
- [ ] 5. Docker Compose (MinIO + Postgres)

### Data Pipeline
- [ ] 6. MinIO → Memory → MinIO
- [ ] 7. MinIO → PostgreSQL
- [ ] 8. PostgreSQL → AWS S3
- [ ] 9. AWS S3 → MinIO
- [ ] 10. Security best practices

### Lambda Functions
- [ ] 11. GET Lambda (random number)
- [ ] 12. POST Lambda (echo body)

### Git Workflow
- [ ] 13-25. Complete Git workflow

### Logging
- [ ] 26-28. Structured logging (JSON + file)

## Setup
```bash
# Create virtual environment
python -m venv .venv

# Activate
.venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run
python main.py
```

## Data
- Source: NPPES NPI Registry
- Size: ~9.9 GB
- Records: 8,858,525 providers
- Focus: 45,546 cardiology specialists
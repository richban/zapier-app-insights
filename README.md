# Zapier App Insights API Platform


## Overview

End-to-end data platform that transforms daily snapshots of Zapier app data into analytical insights served via REST API. Built with **Functional Data Engineering** principles (immutability, idempotency, reproducibility).

**Key Components:**
- **Data Pipelines:** Medallion architecture (Bronze → Silver → Gold) on Databricks
- **Insights API:** FastAPI service querying Delta tables via SQL
- **Quality Framework:** Automated validation, deduplication, quarantine tables
- **Orchestration**: Regular ETL + Delta Live Tables (demonstrates orchestration versatility)

**Tech Stack:** PySpark, Delta Lake, FastAPI, Databricks SQL Connector

### Part A: Model & Transform ✅
- **Data exploration** - Analyzed nested JSON schema, identified natural keys
- **Transformations** - Deduplication, null handling, category flattening, surrogate key generation
- **Data models** - Silver (normalized), Gold (aggregated), Bronze - Quarantine (quality failures)

### Part B: Insights API ✅
- **3+ Insights implemented:**
  1. Premium vs Free analysis (with percentages)
  2. Category-level metrics (app counts, popularity, age)
  3. App health assessment (4 risk segments: high risk, rising stars, featured underperformers, beta graduation)
- **Queryable via SQL** - All endpoints query Delta tables
- **Correctness focus** - Type-safe models, validated responses, health checks

### Part C: Documentation ✅
- **Setup instructions** - This README + detailed `api/README.md`  +  `./docs`
- **Architecture reasoning** - Medallion layers, idempotency, surrogate keys
- **Assumptions & tradeoffs** - Documented in PRD "What's NOT Implemented" section
- **Production steps** - Security, reliability, observability requirements outlined below

### Part D: Future Work ⚠️
- Schema evolution framework
- Comprehensive test suite (unit + integration + E2E)
- CI/CD automation with deployment pipeline
- Advanced observability (tracing, metrics, dashboards)
- Scheduling

---

## Quick Start

### 1. Data Pipeline (Databricks)

**Option A: Regular ETL Notebook**
```bash
# In Databricks workspace
# Run: notebooks/app-insights.py
# Set widget: snapshot_date = "20251030"
# Creates: silver_apps_daily_snapshot, gold_app_metrics_daily, gold_category_metrics_daily
```

**Option B: Delta Live Tables** -
```bash
# Deploy DLT pipeline: pipelines/dlt_pipeline.py
# Configure: snapshot_date, catalog, schema in pipeline settings
# Includes automated quality expectations and data lineage
```

### 2. Insights API (Local)

**API Endpoints:**
- `GET /health` - Data freshness & connectivity
- `GET /insights/premium-analysis` - Premium vs free breakdown
- `GET /insights/categories` - Category-level metrics
- `GET /insights/app-health-assessment` - Risk segments (high risk, rising stars, featured underperformers, beta graduation)

See [`api/README.md`](api/README.md) for detailed API documentation.



## Architecture Highlights

### Data Processing (Medallion)

**Bronze Layer** (JSON files in Databricks Volumes)
- Immutable daily snapshots from `/Volumes/interview_data_pde/app_insights_assignment/raw_apps/YYYYMMDD/*.json`
- Deduplication by `slug` (natural key), keeping highest popularity
- Quality validation with quarantine table for failed records

**Silver Layer** (`silver_apps_daily_snapshot`)
- Cleaned, normalized app records
- SHA2 deterministic surrogate keys (`slug || snapshot_date`)
- Flattened nested JSON (categories array)
- Partitioned by `snapshot_date` for idempotent overwrites

**Gold Layer** (Pre-aggregated metrics)
- `gold_app_metrics_daily` - Daily app-level aggregations
- `gold_category_metrics_daily` - Category-level statistics
- Optimized for API query performance


### Data Flow (DAG)

![](./docs/dag.png)

### API Design

**FastAPI** service with:
- **Type safety:** Pydantic v2 models for request/response validation
- **Auto-documentation:** OpenAPI (Swagger) & ReDoc
- **Structured logging:** Production-ready observability (structlog)
- **Direct SQL queries:** Databricks SQL connector (no ORM overhead)


## Project Structure

```
├── api/                        # FastAPI service
│   ├── src/zapier_insights/    # Application code
│   │   ├── main.py             # FastAPI app entry point
│   │   ├── routers/            # Endpoint handlers
│   │   │   ├── health.py       # Health check endpoint
│   │   │   └── insights.py     # Insights endpoints (3 implemented)
│   │   ├── models.py           # Pydantic response models
│   │   ├── database.py         # Databricks SQL connector
│   │   └── config.py           # Environment-based settings
│   ├── tests/                  # Test suite (pytest)
│   └── README.md               # Detailed API documentation
│
├── notebooks/
│   └── app-insights.py         # Regular ETL notebook (Bronze → Silver → Gold)
│
├── pipelines/
│   ├── dlt_pipeline.py         # Delta Live Tables implementation
│   └── README.md               # Pipeline documentation
│
└── docs/
    ├── PRD.md                  # Product requirements
    └── BRONZE_DATA_FORMAT.md  # Source data schema reference
```


## Production Considerations & 

**What's Missing for Production:**


### Future Extensions (Scoped Out)

#### Data

- [ ] **Schema evolution framework** - Graceful handling of upstream changes
- [ ] **Data quality dashboard** - Completeness scores, freshness SLAs, automated validation

#### Security
- [ ] Authentication with rate limiting
- [ ] Input validation for SQL injection (currently using string interpolation)
- [ ] Access Control - Row levelel isolation

#### Reliability
- [ ] Comprehensive error handling
- [ ] Comprehensive test suite (unit + integration + E2E)

#### Observability
- [ ] RED metrics (rate, errors, duration)
- [ ] Alerting integration (PagerDuty, Slack)

#### Performance
- [ ] Redis caching layer with TTL
- [ ] Cursor-based pagination for large results

#### Testing
- [ ] Unit tests for transformations
- [ ] Integration tests for API endpoints
- [ ] Data quality tests for pipeline validations

#### Deployment
- [ ] Docker containerization
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Infrastructure as Code (Terraform)


---


## AI/LLM Usage Disclosure

**Tools Used:** Claude Code (Anthropic)

**Application Areas:**
1. **Boilerplate generation** - FastAPI router structure, Pydantic models, config templates, Delta Live Tables
2. **Documentation** - README templates, API endpoint documentation

**All AI-generated outputs:**
- ✅ Reviewed and validated for correctness
- ✅ Understood and owned by author

---

**Submission Date:** October 31, 2025
**Time Invested:** ~4 hours (focused on core functionality + documentation)

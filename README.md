[README.md](https://github.com/user-attachments/files/27177727/README.md)
# 🚕 AI-Dataset Engineering Platform (NYC Taxi)

> A robust, production-grade Data Engineering Platform designed to handle the end-to-end lifecycle of the NYC Taxi dataset. This project demonstrates advanced data orchestration, transformation, and validation using the modern data stack.

---

## 🏗️ Architecture Overview

The project follows the **Medallion Architecture** to ensure high data quality and reliability:

```
Raw → Processed → Curated
```

Each layer adds structure, quality, and business value to the data.

---

## 📁 Folder Structure

```
AI-Dataset-Engineering-Platform/
│
├── ingestion/              # Data acquisition via APIs and collectors
├── data_lake/
│   ├── raw/                # Immutable landing zone for source data
│   ├── processed/          # Cleaned data in optimized formats (Parquet/CSV)
│   └── curated/            # Business-ready data for analytics
├── airflow/                # DAGs for pipeline orchestration
├── spark_jobs/             # Heavy-duty data transformations (PySpark)
├── dbt/
│   └── my_dbt_project/
│       ├── staging/        # Initial cleanup and column renaming
│       └── marts/          # Fact & dimension tables (e.g. fct_taxi_trips)
├── quality_checks/         # Automated data validation (schema & logic)
├── dashboard/              # Streamlit-based data visualization
├── docker/                 # Containerization configs
├── requirements.txt
└── docker-compose.yml
```

### 📂 Directory Details

| Folder | Purpose |
|---|---|
| `ingestion/` | Scripts for data acquisition via APIs and collectors |
| `data_lake/raw/` | Immutable landing zone — source data is never modified here |
| `data_lake/processed/` | Cleaned and optimized data (Parquet/CSV formats) |
| `data_lake/curated/` | Business-ready data for analytics and reporting |
| `airflow/` | DAGs to automate and schedule the pipeline end-to-end |
| `spark_jobs/` | Heavy-duty transformations powered by Apache Spark |
| `dbt/staging/` | Initial SQL cleanup, type casting, and column renaming |
| `dbt/marts/` | Fact and dimension tables (e.g., `fct_taxi_trips`) |
| `quality_checks/` | Automated validation to ensure schema and logic integrity |
| `dashboard/` | Streamlit/Python visualization for data insights |
| `docker/` | Containerization logic for seamless deployment |

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.x |
| Orchestration | Apache Airflow |
| Big Data Processing | Apache Spark (PySpark) |
| SQL Transformation | dbt (Data Build Tool) |
| Database / Warehouse | PostgreSQL |
| Containerization | Docker & Docker Compose |
| Quality Assurance | Great Expectations / Custom Validations |
| Visualization | Streamlit |

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Mishal-A/AI-Dataset-Engineering-Platform.git
cd AI-Dataset-Engineering-Platform
```

### 2. Environment Setup

```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run with Docker

```bash
docker-compose up -d
```

### 4. Execute dbt Transformations

```bash
cd dbt/my_dbt_project
dbt build
```

---

## 📊 Data Insights

The `dashboard/` module provides real-time visualization of:

- 🕐 **Peak trip hours and locations**
- 💰 **Revenue analysis and payment trends**
- ⚙️ **Performance metrics of the data pipeline**

---

## 🗺️ Pipeline Flow

```
[Source Data]
     │
     ▼
[ingestion/]  ──────────────→  data_lake/raw/
                                     │
                               [spark_jobs/]
                                     │
                               data_lake/processed/
                                     │
                                  [dbt/]
                               ┌────┴────┐
                           staging/    marts/
                                     │
                               data_lake/curated/
                                     │
                           [quality_checks/]
                                     │
                              [dashboard/]
```

---

## 👤 Contact

**Mishal Ahmed**
Information Systems Student | Data Engineering Enthusiast

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://www.linkedin.com/in/meshal-ahmed-4a84242b5)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?logo=github)](https://github.com/2027-2003)

---

*Built with ❤️ using the Modern Data Stack*

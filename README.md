# 🧠 DLMDSEDE02 – Batch Data Engineering Pipeline  
## Fraud Detection Data Lake → PySpark Processing → PostgreSQL Warehouse

[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Spark 3.4.1](https://img.shields.io/badge/Spark-3.4.1-orange)](https://spark.apache.org/)
[![License: Educational](https://img.shields.io/badge/license-Educational-lightgrey)](LICENSE)
[![GitHub last commit](https://img.shields.io/github/last-commit/YOUR_USERNAME/Project_Data_Engineering)]()

> A complete microservices-based batch processing pipeline using MinIO, PySpark, PostgreSQL, and Airflow, developed for IU assignment DLMDSEDE02.

---

## 📖 Overview

This project implements a modular, containerized, end-to-end batch data engineering pipeline that ingests raw CSV files, stores them in a Data Lake, processes them using PySpark, and loads structured reports into a PostgreSQL Data Warehouse.

The components work together via Docker Compose, mirroring real-world industry data architectures.

Pipeline Stages:
1. Ingestion – Uploads bankdataset.csv from local storage into MinIO (S3-compatible).
2. Processing – PySpark performs data cleaning and calculates:
   - Daily domain transaction trends  
   - Location performance metrics  
   - Domain-level value leaderboard  
3. Loading – Saves aggregated tables into PostgreSQL.
4. Orchestration – Airflow DAG simulates quarterly scheduled runs.

---

## ✨ Key Features

| Feature | Description |
|--------|-------------|
| MinIO Data Lake | Local S3-like storage for raw CSV ingestion |
| PySpark Transformations | Batch job to clean, transform, and aggregate data |
| PostgreSQL Warehouse | Stores three analytical reporting tables |
| Resilient Services | Automatic retries using `tenacity` |
| Containerized Architecture | Fully isolated microservices with Docker Compose |
| Airflow Scheduling | Quarterly pipeline orchestration |
| Windows-Friendly | Works seamlessly on Windows with Docker Desktop |

---

## 🗂️ Project Structure

Project_Data_Engineering/
│
├── data/
│   └── bankdataset.csv
│
├── sql/
│   └── schema.sql
│
├── airflow/
│   └── dags/
│       └── quarterly_dag.py
│
├── src/
│   ├── ingestion/
│   │   ├── Dockerfile
│   │   └── ingest.py
│   │
│   └── processing/
│       ├── Dockerfile
│       └── spark_job.py
│
├── docker-compose.yml
└── README.md

---

## ⚙️ Technologies Used

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Ingestion and processing scripts |
| PySpark 3.4.1 | Distributed data transformations |
| MinIO | Local S3-based data lake |
| PostgreSQL 13 | Analytical data warehouse |
| Airflow 2.6 | Workflow orchestration |
| SQLAlchemy | DB connection and loading |
| Docker & Docker Compose | Containerized microservices |
| Tenacity | Retry logic for robustness |

---

## 🚀 How to Run the Pipeline

1. Ensure Docker Desktop is running  
2. Place your dataset:
   Project_Data_Engineering/data/bankdataset.csv

3. Build & run all services:
   docker-compose up --build

Expected Output:
- Ingestion → “File safely stored in MinIO”  
- Processing → “Reports are live in PostgreSQL”  

---

## 🗄️ Generated Warehouse Tables

rpt_daily_domain_trends – Daily average value per domain  
rpt_location_performance – Location-level averages  
rpt_domain_leaderboard – Ranked total value by domain  

---

## 🔍 Access Interfaces

MinIO Console → http://localhost:9001  
Airflow UI → http://localhost:8080  
Postgres (psql) → admin_user / secure_password

---

## 🔄 Workflow Diagram (Mermaid)

```mermaid
flowchart LR
    A[Local CSV File\n(bankdataset.csv)] --> B[Ingestion Service\n(Python + MinIO SDK)]
    B --> C[MinIO Data Lake]

    C --> D[Processing Service\n(PySpark)]
    D --> E1[Daily Domain Trends]
    D --> E2[Location Performance]
    D --> E3[Domain Leaderboard]

    E1 --> F[(PostgreSQL Warehouse)]
    E2 --> F
    E3 --> F

    subgraph Airflow
        G[Quarterly DAG]
    end

    G --> B
    G --> D

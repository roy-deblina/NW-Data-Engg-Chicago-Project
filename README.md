# 🏙️ Chicago Business Intelligence Project

## Overview

This repository contains the **backend infrastructure, data processing logic (ETL), and API layer** for the City of Chicago Business Intelligence Project. The goal is to build a robust, scalable, and fully automated data platform on **Microsoft Azure** to support city planning and strategic analysis using data from multiple sources (Taxi, Rideshare, Permits, Health).

The architecture follows a **Hybrid Data Lakehouse** pattern, where raw data is managed in Azure Blob Storage (Lake) and cleaned, denormalized data is stored in PostgreSQL (Warehouse).

---

## 1. Prerequisites and Setup

This project requires a Linux Virtual Machine (**API Server VM**) running on Azure with the following services installed and configured:

* **Cloud Infrastructure:** Azure Virtual Machines (API Server, DB Server) and Azure Blob Storage.
* **Database:** **PostgreSQL 14** with the **PostGIS** extension activated.
* **Orchestration:** **Apache Airflow** (deployed on the API Server VM).
* **ETL Languages:** **Python 3.10+** (with `pandas`, `sqlalchemy`, `psycopg2-binary`, `azure-storage-blob`, `pyarrow`).
* **API:** **Go (Golang)** runtime.

### 1.1 Critical Environment Variables

The ETL and API scripts rely entirely on these environment variables, which must be set in the `api-vm`'s `~/.bashrc` file.

| Variable | Purpose | Value Example |
| :--- | :--- | :--- |
| **AZURE_STORAGE_CONNECTION_STRING** | Access to Azure Blob Storage (bronze/silver containers). | `DefaultEndpointsProtocol=https...` |
| **PG_HOST** | Database Server's Private IP (for connectivity). | `172.16.0.4` |
| **PG_USER** | Database username. | `postgres` |
| **PG_PASSWORD** | Database password. | `********` |
| **AIRFLOW_HOME** | Location of Airflow setup. | `~/airflow` |

---

## 2. Data Pipeline Structure (ETL)

The pipeline is defined in `chicago_dag.py` and runs in distinct layers (Extract $\rightarrow$ Transform $\rightarrow$ Load).

### 2.1 Core Cleaning Philosophy

All data cleaning, type conversion, and de-duplication logic (using the `ROW_NUMBER()` fix) is embedded directly into the Python ETL scripts. This ensures that the data loaded into the PostgreSQL tables is always **clean, unique, and ready for frontend consumption**, eliminating the need for manual SQL commands after the load.

| Layer | Files | Key Action |
| :--- | :--- | :--- |
| **Transform** | `transform_*.py` | Loads raw data, filters bad/old data, corrects data types, performs spatial join, and writes cleaned Parquet to **Silver** storage. |
| **Load** | `load_*.py` | Loads Parquet files from Silver, performs **final global de-duplication** by Primary Key (PK), and enforces all PK/FK constraints in PostgreSQL. |

### 2.2 Trips Pipeline (`fact_trips`)

The trips pipeline combines Taxi and TNP (Transportation Network Provider / Rideshare) data into a single, unified fact table.

| Script | Purpose |
| :--- | :--- |
| `transform_taxi.py` | Cleans Taxi data, calculates geography keys, and saves to Silver. |
| `transform_tnp.py` | Cleans TNP data, calculates geography keys, and saves to Silver. |
| **`load_trips.py`** | **CRITICAL:** Loads data from **BOTH** Silver paths, de-duplicates the combined set by `trip_id`, and adds the Primary Key to `fact_trips`. |

### 2.3 Permits Pipeline (`fact_permits`)

| Script | Purpose |
| :--- | :--- |
| `transform_permits.py` | Cleans Permit data, calculates geography keys, and saves to Silver. |
| `load_permits.py` | Loads data from Silver, de-duplicates by `permit_key`, and adds the Primary Key to `fact_permits`. |

---

## 3. Orchestration & Execution

### 3.1 Airflow DAG (`chicago_dag.py`)

The DAG defines the dependencies to guarantee data quality and integrity.

**Dependency Logic Example:**

```python
# Trips Pipeline: Both transformations must finish before the singular, combined load
[task_transform_taxi, task_transform_tnp] >> task_load_trips

# Permits Pipeline (Sequential)
task_transform_permits >> task_load_permits
# ... and so on for all fact tables

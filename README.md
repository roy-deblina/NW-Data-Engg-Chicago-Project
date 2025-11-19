# 🏙️ Chicago Business Intelligence Project

## Overview

This repository contains the **backend infrastructure, data processing logic (ETL), and API layer** for the City of Chicago Business Intelligence Project. The goal is to build a robust, scalable, and fully automated data platform on **Microsoft Azure** to support city planning and strategic analysis using data from multiple sources (Taxi, Rideshare, Permits, Health).

The architecture follows a **Hybrid Data Lakehouse** pattern, where raw data is managed in Azure Blob Storage (Lake) and cleaned, denormalized data is stored in **PostgreSQL** (Warehouse).

***

## 1. Prerequisites and Setup

This project requires a **Linux Virtual Machine (API Server VM)** running on Azure with the following services installed and configured:

* **Cloud Infrastructure:** Azure Virtual Machines (API Server, DB Server) and Azure Blob Storage.
* **Database:** **PostgreSQL 14** with the **PostGIS** extension activated.
* **Orchestration:** **Apache Airflow**.
* **ETL Languages:** **Python 3.10+** (with `pandas`, `sqlalchemy`, `psycopg2-binary`, `azure-storage-blob`, `pyarrow`).
* **API:** **Go (Golang)** runtime.

### 1.1 Critical Foundation (Dimensional Data)

The entire system relies on the existence of two dimension tables, which are populated before the main pipelines run:

* **`dim_date`**: Created by **`populate_dates.py`** and contains every date from 2010 onwards for time-series analysis.
* **`dim_geography`**: Created via initial setup scripts and contains all Chicago boundaries (ZIP Code, Community Area, Neighborhood) used for **spatial joining** and linking all fact data.

### 1.2 Critical Environment Variables

The ETL and API scripts rely entirely on these environment variables, which must be set in the `api-vm`'s `~/.bashrc` file.

| Variable | Purpose | Value Example |
| :--- | :--- | :--- |
| **AZURE_STORAGE_CONNECTION_STRING** | Access to Azure Blob Storage (bronze/silver containers). | `DefaultEndpointsProtocol=https...` |
| **PG_HOST** | Database Server's Private IP (for connectivity). | `172.16.0.4` |
| **PG_USER** | Database username. | `postgres` |
| **PG_PASSWORD** | Database password. | `********` |
| **AIRFLOW_HOME** | Location of Airflow setup. | `~/airflow` |

***

## 2. Data Pipeline Structure (ETL)

The **`chicago_dag.py`** file orchestrates the entire E-T-L process for all datasets. The Python scripts contain all necessary logic for de-duplication, data type correction, and filtering to ensure the database remains clean.

### 2.1 Core Cleaning Philosophy

All data cleaning, type conversion, and de-duplication logic (using the `ROW_NUMBER()` fix) is embedded directly into the Python ETL scripts. This ensures that the data loaded into the database is always **clean, unique, and ready for frontend consumption**, eliminating the need for manual SQL commands after the load.

| Layer | Files | Key Action |
| :--- | :--- | :--- |
| **Transform** | `transform_*.py` | Loads raw data, filters bad/old data, corrects data types, performs **spatial join**, and writes cleaned Parquet to **Silver** storage. |
| **Load** | `load_*.py` | Loads Parquet files from Silver, performs **final global de-duplication** by Primary Key (PK), and enforces all PK/FK constraints in PostgreSQL. |

### 2.2 Pipeline Structure and Dependencies

The Airflow DAG defines clear dependencies to ensure integrity: parallel transformation tasks feed into singular, sequential load tasks.

| Fact Table | Transform Scripts (Parallel) | Load Script (Sequential) | Core Dependency |
| :--- | :--- | :--- | :--- |
| **`fact_trips`** | `transform_taxi.py`, `transform_tnp.py` | `load_trips.py` | `[Taxi T, TNP T] >> Load Trips` (Combines both sources) |
| **`fact_permits`** | `transform_permits.py` | `load_permits.py` | `Transform Permits >> Load Permits` |
| **`fact_covid_daily`** | `transform_covid.py` | `load_covid.py` | `Transform COVID >> Load COVID` |
| **`fact_health_socioeconomic`** | `transform_health.py`, `transform_ccvi.py` | `load_health.py` | `[Health T, CCVI T] >> Load Health` (Combines socioeconomic data) |

***

## 3. Public API (Frontend Handoff)

The Go API runs on **Port 80** of the API Server VM and provides the secure gateway to the data. Frontend applications must use these endpoints and **should not connect directly to the database** (for security reasons).

The available endpoints are defined and handled by the **`server.go`** file listed below.

***

## 4. Repository File List

The project contains the following essential files, organized by function:

| Type | Filename | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | `chicago_dag.py` | The main **Airflow pipeline** definition. |
| **API** | `server.go` | Go HTTP server defining the API endpoints. |
| **Dimension Setup** | `populate_dates.py` | Creates the foundational **`dim_date`** table. |
| **ETL - Trips** | `transform_taxi.py`, `transform_tnp.py` | Cleans, fixes data types, and joins geographical keys for trips. |
| **ETL - Permits** | `transform_permits.py` | Cleans, fixes data types, and joins geographical keys for permits. |
| **ETL - Health/COVID** | `transform_health.py`, `transform_ccvi.py` | Scripts to prepare health and vulnerability index data. |
| **ETL - Loaders** | `load_trips.py`, `load_permits.py`, `load_covid.py`, `load_health.py` | Final scripts to **de-duplicate, enforce Primary Keys**, and load data to the Gold Zone. |
| **Admin** | `check_envs.py` | Utility to verify environment variables. |
| **Documentation** | `README.md` | This file. |

***

## Contact

For any issues related to the cloud environment, database connectivity, or pipeline deployment, please contact **[Your Name] (Cloud Administrator)**.

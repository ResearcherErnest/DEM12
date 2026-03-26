# Architecture & Data Flow

This document details the internal systems architecture and data flow of the DEM12 platform.

## 1. System Context Diagram
Shows the high-level system components and their interactions.

```mermaid
flowchart TD
    User([Platform User / Analyst])
    GEN[Data Generator]
    MINIO[(MinIO Object Storage)]
    AF[Apache Airflow]
    PG[(PostgreSQL<br/>Data Warehouse)]
    MB[Metabase BI]

    User -->|Views Dashboards| MB
    User -.->|Triggers| GEN
    GEN -->|Uploads Raw CSVs| MINIO
    AF -->|Continuously Polls & Streams| MINIO
    AF -->|Transforms & Loads| PG
    MB -->|Executes SQL Queries| PG
    AF -->|Archives Files| MINIO
```

## 2. Containerized Component Architecture
Highlights the Docker services and internal networking.

```mermaid
flowchart LR
    subgraph Docker Network: platform_network
        direction TB
        
        subgraph Compute
            AF_W[Airflow Webserver: 8080]
            AF_S[Airflow Scheduler]
            GEN_C[Data Generator]
            MB_APP[Metabase App: 3000]
            MB_INIT[Metabase Init]
        end
        
        subgraph Storage & Persistence
            PG_DB[(Postgres 16: 5432)]
            MINIO_DB[(MinIO S3: 9000)]
        end

        AF_S -->|Reads/Writes| MINIO_DB
        AF_S -->|Bulk Upserts| PG_DB
        MB_APP -->|Reads| PG_DB
        MB_INIT -->|Configures| MB_APP
        GEN_C -->|Uploads| MINIO_DB
    end
```


## 3. Data Flow Execution Diagram
Details the sequence of Airflow ETL processes.

```mermaid
sequenceDiagram
    participant MinIO
    participant Airflow
    participant Postgres
    
    Airflow->>MinIO: discover_files (List Objects)
    MinIO-->>Airflow: List of pending CSVs
    
    Airflow->>MinIO: request HEAD/Chunk (validate_csv)
    MinIO-->>Airflow: First 10kb data
    
    alt Validation Failed
        Airflow->>MinIO: Move to invalid-data bucket
        Airflow->>Postgres: Log to data_quality_log
    else Validation Passed
        Airflow->>MinIO: Stream specific CSV (transform_data)
        MinIO-->>Airflow: Stream Data chunks
        Airflow->>Airflow: Process in 20k row batches (Pandas)
        Airflow->>Airflow: Save intermediate Parquet parts
        Airflow->>Postgres: bulk_upsert to Dimensions/Facts
        Airflow->>Postgres: Log success to pipeline_runs
        Airflow->>MinIO: Move to processed-data bucket
    end
```

---

## Data Model

![Database Schema](screenshots/dbschema.png)

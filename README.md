This project demonstrates a complete cloud-based data engineering pipeline built using the Microsoft Azure ecosystem. It implements a scalable, secure, and automated workflow to ingest, store, process, and analyze enterprise-level data.
The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) and showcases how Azure services work together to deliver high-quality, analytics-ready datasets.


                                                         **   Architecture.**

![architecture](https://github.com/user-attachments/assets/ef6056b7-02b3-4355-aa71-77b08bf0a147)



azure-data-engineering-project/
│
├── data/
│   
├── notebooks/
│   ├── ingest_sales.py
│   ├── bronze_layer.ipynb
│   ├── silver_transform.py
│   ├── gold_aggregations.py
│   └── qa_checks.py
│
├── pipelines/
│   ├── adf_arm_template.json
│   ├── etl_pipeline.json
│   └── linked_services.json
│
├── synapse/
│   ├── create_external_table.sql
│   ├── analytics_queries.sql
│   └── gold_reporting.sql
│
│
└── README.md



Technologies Used
Azure Data Factory (ADF) – Orchestration & ingestion
Azure Data Lake Storage Gen2 (ADLS) – Central data lake
Azure Databricks (Spark + Delta Lake) – Data processing & transformations
Azure Synapse Analytics – SQL analytics & reporting
Azure Key Vault – Secrets management
Azure Monitor / Log Analytics – Monitoring and logging
Power BI – Analytics & visualization

 Architecture (Bronze → Silver → Gold)
Bronze Layer
Raw data ingestion
Stores original data in native format
Supports CSV, JSON, API outputs, logs, and database extracts
Silver Layer
Cleaned, validated, standardized data
Schema enforcement
Deduplication, null handling, type conversions
Converted to Parquet/Delta
Gold Layer
Business-ready curated data
Fact & dimension models
Aggregates for reporting and dashboards


 Pipeline Workflow
✔Data Ingestion (ADF)
Ingests data from:
On-prem SQL
SaaS APIs
CSV/JSON files
Parameterized pipelines
Scheduled triggers
Linked services & datasets
✔ Data Storage (ADLS Gen2)
Raw → Clean → Curated zone structure
Hierarchical namespace
RBAC + ACL security applied
✔ Data Transformation (Azure Databricks)
PySpark for distributed processing
Auto Loader for incremental ingestion
Delta Lake for:
ACID transactions
Time Travel
Schema enforcement
Transforms Bronze → Silver → Gold tables
✔ SQL Analytics (Azure Synapse)
Connected to Gold layer
External tables
Aggregations, KPIs, insights
Supports Power BI dashboards


Key Features
✔ Fully automated end-to-end pipeline
✔ Medallion Architecture implementation
✔ Scalable distributed processing using Databricks
✔ Secure design with Key Vault & RBAC
✔ Schema drift handling
✔ Incremental & batch processing
✔ Logging, monitoring, alerting
✔ Analytics-ready curated datasets


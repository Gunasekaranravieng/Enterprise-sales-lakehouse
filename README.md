# 🏢 Enterprise Sales Lakehouse

![Azure](https://img.shields.io/badge/Microsoft_Azure-Data_Engineering-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?style=flat-square&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion_Architecture-00ADD8?style=flat-square)
![ADF](https://img.shields.io/badge/Azure_Data_Factory-Orchestration-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Analytics-336791?style=flat-square)
![Power BI](https://img.shields.io/badge/Power_BI-Reporting-F2C811?style=flat-square&logo=powerbi&logoColor=black)

> Production-style Enterprise Sales Lakehouse portfolio project designed using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, SQL and Power BI.

---

## 📌 Project Overview

The **Enterprise Sales Lakehouse** is an end-to-end Data Engineering portfolio project designed to demonstrate how enterprise sales data can be ingested, processed, validated, modelled and prepared for analytics using modern Azure and Databricks technologies.

The solution follows the **Medallion Architecture**:

**Bronze → Silver → Gold**

The project focuses on practical Data Engineering concepts including:

- Data ingestion
- ETL / ELT pipelines
- Azure Data Factory orchestration
- Azure Data Lake Storage
- Databricks and PySpark transformations
- Delta Lake
- Medallion Architecture
- Incremental data processing
- Data quality validation
- Dimensional modelling
- Business KPI generation
- Audit logging
- Monitoring
- SQL analytics
- Power BI consumption

---

## 🎯 Business Scenario

An enterprise organization receives sales information from multiple operational sources.

The platform needs to process datasets such as:

- Customers
- Products
- Stores
- Orders
- Order Items
- Sales Transactions

Raw operational data may contain:

- Duplicate records
- Missing values
- Invalid data
- Incorrect data types
- Inconsistent formats
- Late-arriving records
- Updated business records

The objective is to create a scalable Lakehouse platform that converts raw enterprise data into trusted, analytics-ready datasets.

---

## 🏗️ Solution Architecture

```text
                    ┌─────────────────────────┐
                    │   Enterprise Sources    │
                    │ CSV / Operational Data  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Azure Data Factory    │
                    │ Ingestion & Orchestration│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       ADLS Gen2         │
                    │     Raw Data Storage    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │         BRONZE LAYER          │
                 │ Raw / Historical Data         │
                 └───────────────┬───────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Azure Databricks      │
                    │   PySpark Processing    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │         SILVER LAYER          │
                 │ Cleaned / Validated Data      │
                 └───────────────┬───────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       Delta Lake        │
                    │ Business Transformation │
                    └────────────┬────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │          GOLD LAYER           │
                 │ Analytics-Ready Data          │
                 └───────────────┬───────────────┘
                                 │
                         ┌───────┴────────┐
                         ▼                ▼
                        SQL            Power BI
                         │                │
                         └───────┬────────┘
                                 ▼
                         Business Analytics
```

---

## 🛠️ Technology Stack

| Area | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Orchestration | Azure Data Factory |
| Data Lake | Azure Data Lake Storage Gen2 |
| Data Processing | Azure Databricks |
| Distributed Processing | Apache Spark / PySpark |
| Lakehouse Storage | Delta Lake |
| Programming | Python |
| Query Language | SQL |
| Architecture | Medallion Architecture |
| Analytics | Power BI |
| Version Control | Git / GitHub |

---

## 🥉 Bronze Layer — Raw Data

The Bronze layer is responsible for preserving source data with minimal transformation.

### Responsibilities

- Ingest source datasets
- Preserve original records
- Maintain historical information
- Add ingestion metadata
- Track source information
- Support downstream reprocessing

### Planned Bronze Tables

```text
bronze_customers
bronze_products
bronze_stores
bronze_orders
bronze_order_items
```

Typical metadata fields:

```text
ingestion_timestamp
source_file
batch_id
pipeline_run_id
```

---

## 🥈 Silver Layer — Clean & Validated Data

The Silver layer converts Bronze data into standardized and trusted enterprise datasets.

### Processing

- Schema validation
- Data type conversion
- Duplicate removal
- Null handling
- Date standardization
- String normalization
- Invalid-record handling
- Business-rule validation
- Derived columns
- Referential integrity checks

### Planned Silver Tables

```text
silver_customers
silver_products
silver_stores
silver_orders
silver_order_items
```

### Example PySpark Transformation

```python
from pyspark.sql import functions as F

clean_orders = (
    orders_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"))
    )
)
```

---

## 🥇 Gold Layer — Analytics-Ready Data

The Gold layer contains curated datasets optimized for reporting and business analytics.

The planned Gold model follows dimensional modelling principles.

### Dimension Tables

```text
dim_customer
dim_product
dim_store
dim_date
```

### Fact Table

```text
fact_sales
```

### Star Schema

```text
                 ┌─────────────────┐
                 │  dim_customer   │
                 └────────┬────────┘
                          │
                          │
┌───────────────┐         │         ┌───────────────┐
│  dim_product  │─────────┼─────────│   dim_store   │
└───────────────┘         │         └───────────────┘
                          │
                   ┌──────▼──────┐
                   │ fact_sales  │
                   └──────┬──────┘
                          │
                 ┌────────▼────────┐
                 │    dim_date     │
                 └─────────────────┘
```

---

## 🔄 Incremental Processing

The architecture is designed to support incremental data processing instead of reprocessing the complete dataset during every pipeline execution.

```text
Source
   │
   ▼
Watermark / Last Processed Value
   │
   ▼
Identify New or Updated Records
   │
   ▼
ADF Incremental Ingestion
   │
   ▼
Bronze Layer
   │
   ▼
PySpark Transformation
   │
   ▼
Delta MERGE
   │
   ▼
Silver / Gold
```

### Delta MERGE Concept

```sql
MERGE INTO silver_orders AS target
USING staging_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

This pattern supports both newly created and updated business records.

---

## 🧪 Data Quality Framework

The solution includes a planned Data Quality framework to prevent invalid records from entering trusted datasets.

### Validation Rules

| Check | Purpose |
|---|---|
| Null Check | Validate mandatory fields |
| Duplicate Check | Prevent duplicate business records |
| Type Check | Validate expected data types |
| Range Check | Detect invalid numerical values |
| Date Check | Validate transaction dates |
| Referential Check | Validate entity relationships |
| Business Rule Check | Detect invalid business records |

### Example

```python
valid_sales = (
    sales_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
    .dropDuplicates(["order_id", "product_id"])
)
```

Invalid records can be isolated for investigation rather than silently entering curated datasets.

---

## 🚨 Error Handling & Quarantine

The architecture separates valid and invalid records.

```text
Incoming Data
      │
      ▼
   Validation
    ┌──┴──┐
    │     │
  Valid Invalid
    │     │
    ▼     ▼
 Silver  Quarantine
```

Potential failure scenarios include:

- Source unavailable
- Invalid schema
- Corrupted records
- Transformation failure
- Data quality failure
- Storage failure
- Notebook failure

---

## ⚙️ Azure Data Factory Orchestration

Azure Data Factory is designed to act as the orchestration layer.

### Planned Pipeline Flow

```text
Lookup Configuration
        │
        ▼
Get Metadata
        │
        ▼
Copy Activity
        │
        ▼
Bronze Storage
        │
        ▼
Databricks Notebook
        │
        ▼
Silver Transformation
        │
        ▼
Gold Transformation
        │
        ▼
Audit / Monitoring
```

ADF responsibilities include:

- Source ingestion
- Pipeline orchestration
- Parameterization
- Incremental-load control
- Databricks notebook execution
- Dependency management
- Failure handling
- Pipeline monitoring

---

## 📋 Audit & Monitoring

A production-oriented Data Engineering pipeline should capture operational metadata for every execution.

### Planned Audit Fields

```text
pipeline_name
pipeline_run_id
batch_id
source_name
start_timestamp
end_timestamp
records_read
records_written
records_rejected
pipeline_status
error_message
```

Typical status values:

```text
STARTED
SUCCESS
FAILED
```

This provides traceability across pipeline executions.

---

## 📊 Business KPIs

The Gold layer is designed to support analytics such as:

### Sales KPIs

- Total Revenue
- Total Orders
- Total Units Sold
- Average Order Value
- Average Selling Price

### Product KPIs

- Revenue by Product
- Units Sold by Product
- Top Performing Products
- Product Category Performance

### Customer KPIs

- Revenue by Customer
- Customer Purchase Frequency
- Customer Order Value
- Repeat Customer Analysis

### Regional KPIs

- Revenue by Store
- Revenue by Region
- Store Performance
- Regional Sales Contribution

### Time-Based KPIs

- Daily Revenue
- Monthly Revenue
- Month-over-Month Growth
- Year-over-Year Analysis

---

## 🧮 Example SQL Analytics

### Product Revenue

```sql
SELECT
    product_name,
    SUM(quantity * unit_price) AS total_revenue
FROM fact_sales
GROUP BY product_name
ORDER BY total_revenue DESC;
```

### Monthly Revenue

```sql
SELECT
    YEAR(order_date) AS sales_year,
    MONTH(order_date) AS sales_month,
    SUM(sales_amount) AS monthly_revenue
FROM fact_sales
GROUP BY
    YEAR(order_date),
    MONTH(order_date)
ORDER BY
    sales_year,
    sales_month;
```

---

## 📈 Power BI Consumption

Gold-layer datasets are designed for downstream analytics and reporting.

### Planned Dashboard

```text
Executive Sales Overview
        │
        ├── Total Revenue
        ├── Total Orders
        ├── Average Order Value
        ├── Monthly Revenue Trend
        ├── Top Products
        ├── Regional Performance
        └── Customer Performance
```

---

## ⚡ Performance Considerations

The Lakehouse design considers techniques such as:

- Incremental processing
- Delta Lake MERGE
- Partition pruning
- Predicate filtering
- Efficient Spark transformations
- Appropriate file sizing
- Broadcast joins where appropriate
- Caching only where beneficial
- Optimized Gold aggregations

---

## 🔐 Security Design

A production Azure implementation should follow security practices such as:

- Azure Key Vault for secrets
- Managed Identities
- Role-Based Access Control
- Least-privilege access
- Secure ADLS permissions
- Databricks secret management
- No credentials in source code

Sensitive credentials must never be committed to GitHub.

---

## 📁 Target Repository Structure

```text
Enterprise-sales-lakehouse/
│
├── data/
│   └── sample/
│       ├── customers.csv
│       ├── products.csv
│       ├── stores.csv
│       ├── orders.csv
│       └── order_items.csv
│
├── adf/
│   ├── pipelines/
│   └── datasets/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_model.py
│   └── 04_data_quality.py
│
├── sql/
│   └── business_queries.sql
│
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   └── pipeline_design.md
│
├── screenshots/
│
├── .gitignore
├── requirements.txt
├── LICENSE
└── README.md
```

---

## 🚀 Engineering Goals

This project is designed to demonstrate:

- End-to-end Data Engineering architecture
- Azure Data Factory pipeline design
- Azure Data Lake Storage concepts
- Databricks and PySpark processing
- Delta Lake
- Medallion Architecture
- ETL / ELT
- Incremental processing
- Data quality engineering
- Dimensional modelling
- SQL analytics
- Audit and monitoring
- Production-oriented engineering practices

---

## 🗺️ Implementation Roadmap

- [ ] Generate enterprise sales sample datasets
- [ ] Implement Bronze ingestion
- [ ] Implement Silver transformations
- [ ] Implement Gold dimensional model
- [ ] Implement Data Quality framework
- [ ] Implement incremental processing
- [ ] Add SQL business queries
- [ ] Add ADF pipeline documentation
- [ ] Add architecture diagram
- [ ] Add audit and monitoring logic
- [ ] Add validated execution screenshots
- [ ] Add CI validation workflow
- [ ] Complete final project validation

---

## 📌 Project Status

### 🚧 In Development

This repository currently documents the target architecture and implementation plan for the **Enterprise Sales Lakehouse**.

Actual implementation files, executed outputs and validation evidence will be added as the project is developed.

Cloud services, pipelines or outputs are **not presented as executed unless they have genuinely been implemented and validated**.

---

## 👨‍💻 Author

### Gunasekaran Ravi

**Azure Data Engineer | Databricks | PySpark | Azure Data Factory | Delta Lake | SQL**

🌐 **Portfolio:**  
[Gunasekaran Ravi — Data Engineer](https://gunasekaran-ravi-portfolio.vercel.app/)

💼 **LinkedIn:**  
[Gunasekaran Ravi](https://www.linkedin.com/in/gunasekaran-ravi-938792403/)

📍 Chennai, India

---

## 📄 License

---

# 📸 Execution Results

The following screenshots provide execution evidence from the implemented **Enterprise Sales Lakehouse** workflow in Databricks.

They demonstrate source generation, Bronze ingestion, Silver transformations, Gold analytics, incremental processing, data quality validation, monitoring, performance analysis, and final end-to-end validation.

> **Implementation note:** The processing workflow was executed using Databricks. Azure Data Factory and ADLS Gen2 are represented as the target Azure production architecture and are not presented as executed cloud components.

---

## 1️⃣ Source Data Generation

Representative enterprise sales datasets were generated and validated before entering the Medallion Architecture.

![Customer Source Data](screenshots/01_customer_source_data.png)

![Source Data Quality Validation](screenshots/02_source_data_quality_validation.png)

---

## 2️⃣ Bronze Layer — Raw Ingestion

The Bronze layer preserves source records and adds ingestion metadata for traceability.

![Bronze Orders with Metadata](screenshots/04_bronze_orders_with_metadata.png)

![Bronze Reconciliation](screenshots/05_bronze_reconciliation.png)

![Bronze Ingestion Summary](screenshots/06_bronze_ingestion_summary.png)

---

## 3️⃣ Silver Layer — Transformation & Trusted Data

Bronze data is cleaned, standardized, validated, and transformed into trusted Silver datasets.

![Silver Orders Transformed](screenshots/07_silver_orders_transformed.png)

![Silver Trusted Sales](screenshots/08_silver_trusted_sales.png)

![Silver Data Quality](screenshots/09_silver_data_quality.png)

![Silver Transformation Summary](screenshots/10_silver_transformation_summary.png)

---

## 4️⃣ Gold Layer — Business Analytics

The Gold layer provides analytics-ready sales facts, KPIs, and regional business insights.

![Gold Fact Sales](screenshots/11_gold_fact_sales.png)

![Gold Business KPIs](screenshots/12_gold_business_kpis.png)

![Gold Regional Performance](screenshots/13_gold_regional_performance.png)

![Gold Analytics Summary](screenshots/14_gold_analytics_summary.png)

---

## 5️⃣ Incremental Processing — Delta MERGE

Incremental processing demonstrates insert/update handling and idempotent Delta Lake MERGE behavior.

![Incremental Sales Batch](screenshots/15_incremental_sales_batch.png)

![Incremental MERGE Validation](screenshots/16_incremental_merge_validation.png)

![Incremental Processing Summary](screenshots/17_incremental_processing_summary.png)

---

## 6️⃣ Enterprise Data Quality & Reconciliation

Cross-layer validation verifies data quality, reconciliation, and referential integrity.

![Enterprise Data Quality Checks](screenshots/18_enterprise_data_quality_checks.png)

![Layer Reconciliation](screenshots/19_layer_reconciliation.png)

![Data Quality Validation Summary](screenshots/20_data_quality_validation_summary.png)

---

## 7️⃣ Audit & Monitoring

Operational metrics and health checks provide visibility into pipeline execution and data health.

![Layer Audit Metrics](screenshots/21_layer_audit_metrics.png)

![Pipeline Health Monitoring](screenshots/22_pipeline_health_monitoring.png)

![Audit Monitoring Summary](screenshots/23_audit_monitoring_summary.png)

---

## 8️⃣ Performance & Business Analytics

Spark-based analytical processing demonstrates optimized aggregation, execution planning, and business analytics.

![Optimized Daily Sales](screenshots/24_optimized_daily_sales.png)

![Spark Execution Plan](screenshots/25_spark_execution_plan.png)

![Performance Analytics Summary](screenshots/26_performance_analytics_summary.png)

---

## 9️⃣ End-to-End Project Validation

The final validation verifies required Lakehouse tables, processing stages, data integrity, monitoring health, and overall project completion.

![Required Tables Validation](screenshots/27_required_tables_validation.png)

![Final Validation Matrix](screenshots/28_final_validation_matrix.png)

![Project Validation](screenshots/29_project_validation_100_percent.png)

---

## ✅ Implementation Summary

| Capability | Status |
|---|---|
| Source Data Generation | ✅ Implemented |
| Bronze Layer | ✅ Implemented |
| Silver Layer | ✅ Implemented |
| Gold Layer | ✅ Implemented |
| Delta Lake Storage | ✅ Implemented |
| Incremental Delta MERGE | ✅ Implemented |
| Data Quality Validation | ✅ Implemented |
| Layer Reconciliation | ✅ Implemented |
| Referential Integrity Validation | ✅ Implemented |
| Audit & Monitoring | ✅ Implemented |
| Spark Performance Analysis | ✅ Implemented |
| Business Analytics | ✅ Implemented |
| End-to-End Validation | ✅ Implemented |
| Azure Data Factory | 📐 Target Architecture / Design |
| Azure Data Lake Storage Gen2 | 📐 Target Architecture / Design |

---

## 🏆 Final Result

The implemented Databricks Lakehouse workflow successfully demonstrates:

**Source → Bronze → Silver → Gold → Incremental Processing → Data Quality → Reconciliation → Monitoring → Performance Analytics → Final Validation**

The repository contains the executable notebook source, SQL analytics, architecture documentation, pipeline documentation, and execution evidence required to review the project implementation.

This project is intended for educational and portfolio demonstration purposes.

---

### ⭐ Enterprise Data Engineering with Azure, Databricks, PySpark and modern Lakehouse architecture.

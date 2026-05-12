# Big Data Analytics Pipeline for Online Retail Transactions

An end-to-end **Big Data Management and Analytics Pipeline** built using the Hadoop ecosystem. This project processes large-scale online retail transaction data using **HDFS, HBase, Hive, Apache Spark, and PySpark** to perform distributed storage, NoSQL data management, SQL-based analytics, Spark-based analytics, and performance comparison.

The project is based on the **Online Retail II dataset**, containing more than **1 million raw transaction records**. After data cleaning and preprocessing, **805,549 valid transaction records** were used for distributed analytics.

---

## Project Overview

Retail businesses generate large volumes of transactional data from invoices, products, customers, sales, and countries. Traditional single-machine processing becomes inefficient when handling hundreds of thousands or millions of records.

This project solves that problem by designing a complete Hadoop-based data pipeline that can:

- Ingest raw retail transaction data
- Clean and prepare data using Python
- Store raw and processed files in HDFS
- Store cleaned records in HBase as a NoSQL database
- Run SQL-style distributed analytics using Hive
- Run scalable DataFrame-based analytics using Spark/PySpark
- Compare Hive and Spark based on performance, scalability, code complexity, and ease of development

---

## Project Category

**Big Data Analytics | Data Engineering | Distributed Processing | Retail Analytics | Hadoop Ecosystem**

---

## Dataset

The project uses the **Online Retail II dataset** from the UCI Machine Learning Repository.

### Dataset Summary

| Item | Description |
|------|-------------|
| Dataset | Online Retail II |
| Domain | E-commerce transaction analytics |
| Raw Records | 1,067,371 rows |
| Cleaned Records | 805,549 valid transaction records |
| Format | CSV |
| Data Type | Structured transactional data |
| Source | UCI Machine Learning Repository |

### Main Columns

| Column | Description |
|--------|-------------|
| `InvoiceNo` | Invoice number |
| `StockCode` | Product/item code |
| `Description` | Product description |
| `Quantity` | Quantity purchased |
| `InvoiceDate` | Transaction date |
| `UnitPrice` | Price per item |
| `CustomerID` | Customer identifier |
| `Country` | Customer country |
| `Revenue` | Calculated sales revenue |

---

## Technologies Used

- Python
- Pandas
- Apache Hadoop
- HDFS
- HBase
- Apache Hive
- Apache Spark
- PySpark
- HiveQL
- Linux / Ubuntu
- HDFS CLI
- HBase Shell

---

## System Architecture

The system follows a layered big data architecture:

```text
Raw Online Retail Dataset
        |
        v
Python Data Cleaning & Preprocessing
        |
        v
HDFS Distributed Storage
        |
        |--------------------------|
        v                          v
HBase NoSQL Storage             Hive External Table
        |                          |
        v                          v
NoSQL Row-Key Access          SQL-Based Analytics
                                   |
                                   v
                            Spark/PySpark Analytics
                                   |
                                   v
                         Business Insights & Results
```

---

## Data Pipeline Workflow

The complete workflow includes:

1. Download and prepare the Online Retail II dataset
2. Convert raw Excel data into CSV format
3. Clean invalid, missing, cancelled, and inconsistent records
4. Calculate revenue using quantity and unit price
5. Upload raw and cleaned datasets to HDFS
6. Import cleaned records into HBase
7. Create Hive external table over cleaned HDFS data
8. Run Hive SQL analytics queries
9. Run Spark/PySpark DataFrame analytics pipeline
10. Save Spark outputs back to HDFS
11. Compare Hive and Spark performance
12. Generate final business insights

---

## HDFS Storage Layer

HDFS was used as the distributed file storage layer because it is designed for large-scale file storage and integrates directly with Hadoop ecosystem tools such as Hive, Spark, and HBase.

### HDFS Directory Structure

```text
/ds603_project/
│
├── raw/
│   └── online_retail_raw.csv
│
├── clean/
│   └── online_retail_clean.csv
│
└── outputs/
    ├── top_products/
    ├── top_countries/
    ├── monthly_revenue/
    └── top_customers/
```

### Example HDFS Commands

```bash
hdfs dfs -mkdir -p /ds603_project/raw
hdfs dfs -mkdir -p /ds603_project/clean
hdfs dfs -mkdir -p /ds603_project/outputs

hdfs dfs -put online_retail_raw.csv /ds603_project/raw/
hdfs dfs -put online_retail_clean.csv /ds603_project/clean/

hdfs dfs -ls /ds603_project/
hdfs dfs -count /ds603_project/clean/
```

---

## HBase NoSQL Storage

HBase was used as the NoSQL wide-column database because it runs on top of HDFS and supports scalable storage for large datasets.

### HBase Table Design

| Design Item | Value |
|------------|-------|
| Table Name | `retail_transactions` |
| Column Family | `txn` |
| Row Key | `InvoiceNo_StockCode_RowNumber` |
| Imported Records | 805,549 cleaned records |

### Stored Columns

- `invoice_no`
- `stock_code`
- `description`
- `quantity`
- `invoice_date`
- `unit_price`
- `customer_id`
- `country`
- `revenue`

### Example HBase Commands

```bash
hbase shell
```

```sql
create 'retail_transactions', 'txn'

scan 'retail_transactions', {LIMIT => 5}

count 'retail_transactions', {INTERVAL => 100000}

get 'retail_transactions', '489434_21232_4'
```

---

## Hive Analytics Pipeline

Hive was used to perform SQL-style distributed analytics on the cleaned HDFS dataset.

Hive is useful for analysts and data engineers because it allows large-scale data analysis using SQL-like queries.

### Hive Table Creation

```sql
CREATE DATABASE IF NOT EXISTS retail_project;
USE retail_project;

CREATE EXTERNAL TABLE IF NOT EXISTS online_retail (
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate STRING,
    UnitPrice DOUBLE,
    CustomerID STRING,
    Country STRING,
    Revenue DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ds603_project/clean/';
```

---

## Spark / PySpark Analytics Pipeline

Apache Spark was used for scalable DataFrame-based analytics. Spark reads the cleaned CSV file from HDFS, performs filtering, grouping, aggregation, sorting, and saves the output results back to HDFS.

### Example PySpark Logic

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

spark = SparkSession.builder \
    .appName("Online Retail Big Data Analytics") \
    .getOrCreate()

input_path = "hdfs:///ds603_project/clean/online_retail_clean.csv"

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

country_revenue = df.filter(col("Revenue") > 0) \
    .groupBy("Country") \
    .agg(round(sum("Revenue"), 2).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)

country_revenue.show()
```

---

## Analytics Tasks Implemented

Four major business analytics tasks were implemented using both Hive and Spark.

### 1. Top Products by Revenue

Identifies the highest revenue-generating products.

```sql
SELECT Description, ROUND(SUM(CAST(Revenue AS DOUBLE)), 2) AS total_revenue
FROM online_retail
WHERE Revenue IS NOT NULL AND CAST(Revenue AS DOUBLE) > 0
GROUP BY Description
ORDER BY total_revenue DESC
LIMIT 10;
```

### 2. Top Countries by Revenue

Identifies the countries contributing the most revenue.

```sql
SELECT Country, ROUND(SUM(CAST(Revenue AS DOUBLE)), 2) AS total_revenue
FROM online_retail
WHERE Revenue IS NOT NULL AND CAST(Revenue AS DOUBLE) > 0
GROUP BY Country
ORDER BY total_revenue DESC
LIMIT 10;
```

### 3. Monthly Revenue Trend

Analyzes revenue performance over time.

```sql
SELECT SUBSTR(InvoiceDate, 1, 7) AS month,
       ROUND(SUM(CAST(Revenue AS DOUBLE)), 2) AS monthly_revenue
FROM online_retail
WHERE Revenue IS NOT NULL AND CAST(Revenue AS DOUBLE) > 0
GROUP BY SUBSTR(InvoiceDate, 1, 7)
ORDER BY month;
```

### 4. Top Customers by Spending

Identifies the highest-spending customers.

```sql
SELECT CustomerID, ROUND(SUM(CAST(Revenue AS DOUBLE)), 2) AS total_spending
FROM online_retail
WHERE CustomerID IS NOT NULL
  AND Revenue IS NOT NULL
  AND CAST(Revenue AS DOUBLE) > 0
GROUP BY CustomerID
ORDER BY total_spending DESC
LIMIT 10;
```

---

## Key Business Insights

The analytics pipelines generated the following insights:

| Result Area | Main Finding |
|------------|--------------|
| Top Product | REGENCY CAKESTAND 3 TIER |
| Top Country | United Kingdom |
| Highest Revenue Customer | Customer ID 18102 |
| Trend Period | December 2009 to December 2011 |
| Technical Finding | Hive and Spark produced consistent outputs |

---

## Performance Comparison

Hive and Spark were compared based on execution time, code complexity, scalability, and ease of development.

### Main Pipeline Comparison

| Analysis Task | Hive Execution Time | Spark Execution Time |
|--------------|--------------------|----------------------|
| Top 10 products by revenue | 8.29 sec | Included in full Spark pipeline |
| Top 10 countries by revenue | 7.16 sec | Included in full Spark pipeline |
| Monthly revenue trend | 7.20 sec | Included in full Spark pipeline |
| Top 10 customers by spending | 35.11 sec | Included in full Spark pipeline |
| All four analytics tasks | Separate Hive queries | 9.89 sec |

Spark completed all four analytics workflows in a single PySpark pipeline in **9.89 seconds**.

---

## Bonus Experiment: Hive vs Spark on Subsets

A bonus comparative experiment was performed using the same analytics task: **Top 10 countries by revenue**.

The task was executed on three dataset sizes:

- 10,000 rows
- 50,000 rows
- 100,000 rows

### Bonus Results

| Tool | Rows | Execution Time | Notes |
|------|------|----------------|------|
| Hive | 10,000 | 3.56 sec | SQL query over Hive external table |
| Spark | 10,000 | 4.70 sec | PySpark DataFrame aggregation |
| Hive | 50,000 | 3.49 sec | SQL query over Hive external table |
| Spark | 50,000 | 4.54 sec | PySpark DataFrame aggregation |
| Hive | 100,000 | 3.52 sec | SQL query over Hive external table |
| Spark | 100,000 | 4.78 sec | PySpark DataFrame aggregation |

### Observation

Hive was faster for this specific small-scale local experiment because the task was simple SQL-based aggregation. Spark required more setup and code, but it remains more suitable for larger datasets, repeated transformations, complex pipelines, and scalable analytics workflows.

---

## Technology Comparison

### Storage Technology Comparison

| Criteria | HBase | MongoDB | Relational Database |
|---------|-------|---------|--------------------|
| Storage Model | Wide-column NoSQL | Document NoSQL | Relational tables |
| Hadoop Integration | Excellent | Moderate | Limited |
| Scalability | Very high | High | Medium |
| Ease of Implementation | Moderate | Moderate | High |
| Project Suitability | Best fit | Good but less Hadoop-focused | Not suitable for NoSQL requirement |

### Processing Technology Comparison

| Criteria | Hive | Spark | MapReduce | Pig |
|---------|------|-------|-----------|-----|
| Processing Model | SQL batch queries | DataFrame/in-memory | Low-level batch jobs | Data-flow scripting |
| Ease of Development | High | Moderate | Low | Moderate |
| Performance | Good for batch reporting | Fast for pipelines | Slower | Medium |
| Code Complexity | Low | Medium | High | Medium |
| Project Suitability | Strong | Strongest | Useful but complex | Optional |

---

## Why This Project Matters

This project demonstrates practical big data engineering skills required in real-world organizations:

- Handling large-scale structured transaction data
- Designing distributed storage architecture
- Building NoSQL storage with HBase
- Running SQL analytics using Hive
- Building scalable PySpark analytics pipelines
- Comparing big data tools based on performance and usability
- Generating business insights from raw transactional data

---

## Skills Demonstrated

- Big Data Analytics
- Data Engineering
- Hadoop Ecosystem
- HDFS Storage
- HBase NoSQL Database Design
- Hive SQL Analytics
- Spark DataFrame Processing
- PySpark Development
- ETL Pipeline Design
- Data Cleaning and Preprocessing
- Distributed Processing
- Retail Revenue Analytics
- Customer Analytics
- Product Analytics
- Performance Benchmarking
- Technology Comparison
- Business Intelligence Reporting

---

## Project Structure

```text
Big-Data-Analytics-Pipeline-Online-Retail/
│
├── README.md
├── requirements.txt
│
├── data_cleaning/
│   └── clean_online_retail.py
│
├── hbase/
│   ├── create_hbase_table.txt
│   └── import_to_hbase.py
│
├── hive/
│   ├── create_hive_table.sql
│   ├── top_products.sql
│   ├── top_countries.sql
│   ├── monthly_revenue.sql
│   └── top_customers.sql
│
├── spark/
│   └── retail_analytics_pyspark.py
│
├── outputs/
│   ├── top_products/
│   ├── top_countries/
│   ├── monthly_revenue/
│   └── top_customers/
│
├── screenshots/
│   ├── hdfs_ingestion/
│   ├── hbase_storage/
│   ├── hive_results/
│   ├── spark_results/
│   └── comparison/
│
└── report/
    └── Big_Data_Project_Report.pdf
```

---

## How to Run the Project

### 1. Start Hadoop Services

```bash
start-dfs.sh
start-yarn.sh
jps
```

### 2. Upload Dataset to HDFS

```bash
hdfs dfs -mkdir -p /ds603_project/raw
hdfs dfs -mkdir -p /ds603_project/clean

hdfs dfs -put online_retail_raw.csv /ds603_project/raw/
hdfs dfs -put online_retail_clean.csv /ds603_project/clean/
```

### 3. Create HBase Table

```bash
hbase shell
```

```sql
create 'retail_transactions', 'txn'
scan 'retail_transactions', {LIMIT => 5}
```

### 4. Run Hive Queries

```bash
hive -f hive/create_hive_table.sql
hive -f hive/top_products.sql
hive -f hive/top_countries.sql
hive -f hive/monthly_revenue.sql
hive -f hive/top_customers.sql
```

### 5. Run Spark Analytics Pipeline

```bash
spark-submit spark/retail_analytics_pyspark.py
```

### 6. View Spark Outputs in HDFS

```bash
hdfs dfs -ls /ds603_project/outputs/
hdfs dfs -cat /ds603_project/outputs/top_countries/part-*
```

---

## Example Output

```text
Top Country by Revenue: United Kingdom
Top Product by Revenue: REGENCY CAKESTAND 3 TIER
Highest Spending Customer: Customer ID 18102
Cleaned Transaction Records: 805,549
Spark Full Pipeline Runtime: 9.89 seconds
```

---

## Challenges Faced

During implementation, several technical challenges were handled:

- Hadoop native library warning in single-node Ubuntu setup
- HDFS safe mode handling
- HiveServer2 connection configuration
- Spark version compatibility issue
- CSV formatting and quoted-field handling
- Large dataset cleaning and validation
- Matching Hive and Spark outputs for result consistency

---

## Future Improvements

Possible future improvements include:

- Add Apache Airflow for workflow orchestration
- Add dashboard visualization using Power BI, Tableau, or Streamlit
- Store final analytics results in a relational data mart
- Add automated data quality checks
- Use Apache Kafka for real-time transaction ingestion
- Deploy the pipeline on a multi-node Hadoop cluster
- Add machine learning models for customer segmentation or sales forecasting
- Convert processed datasets into Parquet for optimized Spark performance

---

## Conclusion

This project successfully implements a complete big data analytics pipeline using the Hadoop ecosystem. It demonstrates how large-scale retail transaction data can be ingested, cleaned, stored, processed, analyzed, and compared using industry-relevant tools.

The final solution uses **HDFS** for distributed storage, **HBase** for NoSQL data management, **Hive** for SQL-based analytics, and **Spark/PySpark** for scalable analytics pipelines. The project produced meaningful business insights and demonstrated practical technology selection for real-world big data environments.

---

## Author

**Bilal Fayyaz**  
BS Data Science  
FAST NUCES, Islamabad  

GitHub: [github.com/bilalfayyaz66](https://github.com/bilalfayyaz66)

---

## Repository Topics

```text
big-data
hadoop
hdfs
hbase
hive
apache-spark
pyspark
data-engineering
data-analytics
etl
distributed-processing
nosql
retail-analytics
business-intelligence
sql
python
```

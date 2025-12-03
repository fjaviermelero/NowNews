Now News.

Developed by Francisco Melero. www.fjmelero.com

News ETL and Analytics Pipeline

This repository contains a full data engineering pipeline designed to ingest, clean, transform, and analyze news content from multiple online newspapers. The workflow uses Google Cloud services, Python web scraping, and PySpark to produce analytical datasets modeled in a BigQuery star schema.

Architecture Overview

The pipeline is organized into four main phases:

1. Ingestion (Web - Bronze Layer in GCS)

News articles are extracted from online newspapers using a Python web scraping component.
The ingestion step collects raw HTML and text content and stores it directly in the Bronze layer in Google Cloud Storage (GCS), preserving the original structure for traceability.

Technologies:
Python (web scraping)
Cloud Run Functions (optional deployment target)
Google Cloud Storage

2. Cleaning and Standardization (Golden layer in GCS - Silver Layer in GCS)

PySpark processes the raw Bronze data to produce standardized and enriched records.

Typical steps include:
Deduplication
Text cleanup and normalization
Extraction of metadata such as date, title, and newspaper
The cleaned datasets are stored in the Silver layer in GCS, still in a file-based format.

Technologies:
PySpark
Google Cloud Storage

3. Topic Extraction (Gold layer in GCS – BigQuery Staging)

A second PySpark job transforms the Silver data to extract topics using ML techniques such as topic modeling.
The process identifies the main themes discussed across articles.
The resulting topic associations and metrics are written into staging tables in BigQuery, representing the Gold layer.

Technologies:
PySpark (Spark ML)
Google Cloud Storage (input)
BigQuery (staging output)

4. Data Warehouse Modeling (BigQuery Star Schema)

SQL transformations implement the final star-schema data model.

Dimensions:
dim_topics: list of detected ML topics
dim_newspapers: list of source newspapers

Fact table:
date
newspaper_id
topic_id
topic_count (how many times each topic appears in a newspaper on a given date)

This curated structure supports analytical queries and BI dashboards.

Technologies:
BigQuery SQL
Star-schema modeling

*****************************************************
Pipeline Flow Summary

Python web scraping → Bronze (GCS)
PySpark job → Bronze to Silver (GCS)
PySpark ML job → Silver to Gold (BigQuery staging)
SQL transformations → Final star schema in BigQuery
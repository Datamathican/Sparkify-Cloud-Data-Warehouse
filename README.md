# Project: Sparkify Cloud Data Warehouse (AWS Redshift)

## 📋 Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline that extracts data from S3, stages it in Amazon Redshift, and transforms it into a dimensional model (Star Schema). This allows the analytics team to find insights into user song listening habits using simple SQL.

## 🏗️ Data Architecture
The pipeline moves data through three distinct stages:
1. **Data Source:** Raw JSON files stored in public Amazon S3 buckets.
2. **Staging:** Data is "bulk loaded" into Redshift staging tables for high-speed processing.
3. **Analytics (Warehouse):** Data is transformed into a **Star Schema** with Fact and Dimension tables for optimized query performance.

## 📂 Folder Structure
```text
├── dwh.cfg              # Configuration file (AWS Redshift credentials & IAM Role)
├── create_tables.py     # Script to drop and recreate the database schema
├── etl.py               # The main engine: logic to copy and transform data
├── sql_queries.py       # Centralized SQL repository for DDL and DML operations
└── README.md            # Project documentation and instructions
```
## 🛠️ Technologies Used
Python: For scripting the logic and connecting to the database.

Amazon Redshift: The primary Cloud Data Warehouse.

Amazon S3: The data lake storage for raw logs.

SQL: For building the relational model (Postgres-compatible).

## 🗄️ Database Schema (Star Schema)
To make analysis simple for tools like Athena and Excel, the data is organized into a Star Schema:

Fact Table
songplays: Records of song starts (user_id, song_id, session_id, etc.)

Dimension Tables
users: Users in the app.

songs: Songs in the music database.

artists: Artists in the music database.

time: Timestamps of records broken down into specific units (hour, day, week).

## 🚀 How to Run the Project
1. Prerequisites
Ensure you have an active AWS Redshift cluster running and an IAM Role with AmazonS3ReadOnlyAccess.

2. Configure Credentials
Update the dwh.cfg file with your specific cluster details:

```
HOST=your-redshift-endpoint
DB_NAME=dev
DB_USER=awsuser
DB_PASSWORD=yourpassword
DB_PORT=5439
```
3. Build the Schema
Run the table creation script to set up the staging and warehouse tables:

```
python create_tables.py
4. Execute ETL
Run the ETL script to migrate data from S3 into Redshift:
```
```
python etl.py
```
## 📈 Business Impact
Efficiency: Automated the ingestion of millions of rows from JSON to structured SQL.

Scalability: Built on Redshift, meaning the system can grow to Petabytes without a loss in performance.

Accessibility: The team can now connect Excel directly to Redshift to run reports on clean, validated data.# Sparkify-Cloud-Data-Warehouse

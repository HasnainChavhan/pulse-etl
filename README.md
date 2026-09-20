# pulse-etl
Real-Time E-commerce ETL Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Airflow](https://img.shields.io/badge/Airflow-2.x-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue.svg)
![dbt](https://img.shields.io/badge/dbt-Core-orange.svg)

## Architecture
```
[External Sources] -> (Extract) -> [Staging Data] -> (Transform) -> [Clean Data] -> (Load) -> [PostgreSQL DWH] -> (dbt) -> [Data Marts]
```

## Features
- E-commerce fake data generation
- Data quality validation with Great Expectations
- Robust transformation using Pandas
- Upsert logic for loading into PostgreSQL
- dbt models for mart creation
- Fully orchestrated via Apache Airflow

## Setup
1. Copy `.env.example` to `.env` and fill variables.
2. Run `docker-compose up -d`.
3. Access Airflow UI at `http://localhost:8080`.

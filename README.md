# E-Commerce Analytics Data Warehouse

An enterprise-grade data engineering solution implementing real-time web scraping, stream processing, and dimensional modeling for e-commerce analytics using Apache Kafka, Apache Spark, and PostgreSQL.

## Project Information

**Author:** Nguyen Ha Minh Duy


**Technology Stack:** Apache Kafka, Apache Spark, PySpark, PostgreSQL, Python, Docker

## Executive Summary

This project demonstrates the implementation of a comprehensive data engineering pipeline designed to collect, process, and analyze customer behavior data from Glamira, a luxury jewelry e-commerce platform. The system leverages industry-standard technologies to create a scalable, maintainable solution for real-time analytics and business intelligence.

The architecture encompasses end-to-end data flow from the Glamira platform through processing layers to a structured data warehouse, enabling advanced analytics and reporting capabilities for understanding customer interactions with jewelry products, customization options (diamonds, alloys), and purchasing patterns across multiple geographic markets.

## Technical Architecture

### System Components

**Data Collection Layer**
- Apache Kafka message broker for real-time data ingestion
- Web scraping infrastructure for Glamira e-commerce platform integration
- Event-driven architecture for capturing customer interactions with jewelry products
- Support for tracking product customization options (diamond settings, alloy types)

**Data Processing Layer**
- Apache Spark distributed processing engine
- PySpark for ETL operations and data transformations
- Stream processing capabilities for real-time analytics

**Data Storage Layer**
- PostgreSQL relational database management system
- Star schema dimensional model optimized for analytical queries
- Normalized dimension tables with surrogate keys

**Orchestration Layer**
- Custom Python-based workflow management
- Shell scripts for job scheduling and execution
- Logging and monitoring infrastructure

## Project Architecture

### Directory Structure

```
e-commerce-analytics-warehouse/
├── config/                     # Application configuration files
├── data/
│   ├── processed/             # Transformed and cleaned datasets
│   │   └── currency/          # Currency conversion data
│   └── raw/                   # Raw ingested data
├── warehouse/                 # Data warehouse implementation
│   ├── dim_alloy_option.py   # Alloy options dimension
│   ├── dim_currency.py        # Currency dimension
│   ├── dim_customer.py        # Customer dimension
│   ├── dim_date.py            # Date dimension
│   ├── dim_device.py          # Device dimension
│   ├── dim_diamond_option.py  # Diamond options dimension
│   ├── dim_prd.py             # Product dimension
│   ├── dim_product.py         # Product details dimension
│   └── main_schema.py         # Schema orchestration
├── helper/                    # Utility modules
├── pyspark_venv/             # PySpark virtual environment
├── sql/                       # SQL DDL and query scripts
├── src/                       # Application source code
│   ├── load.py               # Data loading operations
│   └── main.py               # Application entry point
├── .env                       # Environment configuration
├── merge_log.txt             # Merge operation audit logs
└── run_crawler_after_postgres.sh  # Pipeline execution script
```

## Data Warehouse Design

### Dimensional Model Specification

The data warehouse implements a star schema design pattern with fact and dimension tables optimized for analytical query performance. The central fact table captures user interaction events from the Glamira platform with foreign keys to dimension tables, enabling analysis of jewelry product views, customization preferences, and customer behavior patterns.

### Event Log Schema

| Attribute | Data Type | Description | Business Rule |
|-----------|-----------|-------------|---------------|
| id | String | Unique event identifier | UUID format, primary key |
| api_version | String | API version identifier | Version control tracking |
| collection | String | Event classification | Categorical: view_product_detail |
| current_url | String | Page URL at event time | Glamira product page URL with parameters |
| device_id | String | Unique device identifier | UUID format, persistent identifier |
| email | String | Authenticated user email | Nullable for anonymous sessions |
| ip | String | Client IP address | IPv4 format |
| local_time | String | Event timestamp (local) | Format: yyyy-MM-dd HH:mm:ss |
| option | Array | Product configuration options | Jewelry customization: diamond settings, alloy types |
| product_id | String | Product identifier | Foreign key to product dimension |
| referer_url | String | HTTP referrer | Source page URL |
| store_id | String | Store identifier | Foreign key to store dimension |
| time_stamp | Long | Unix epoch timestamp | System time in milliseconds |
| user_agent | String | Client user agent string | Browser and device information |

### Dimension Tables

The warehouse implements the following dimension tables following Kimball methodology, specifically designed for luxury jewelry e-commerce analytics:

- **dim_date**: Temporal dimension with date hierarchies
- **dim_product**: Jewelry product master data and attributes
- **dim_customer**: Customer demographics and segments
- **dim_device**: Device characteristics and categories
- **dim_currency**: Currency exchange rates and metadata for international transactions
- **dim_alloy_option**: Product customization options for metal alloys (gold, platinum, silver)
- **dim_diamond_option**: Product customization options for diamond settings and characteristics

## System Requirements

### Infrastructure Prerequisites

- **Operating System:** Linux/Unix-based system (recommended) or Windows with WSL2
- **Python:** Version 3.8 or higher
- **Apache Spark:** Version 3.1.1 or higher
- **Apache Kafka:** Version 2.8.0 or higher with Zookeeper
- **PostgreSQL:** Version 12.0 or higher
- **Docker:** Version 20.10 or higher (for containerized deployment)
- **Memory:** Minimum 8GB RAM (16GB recommended for production workloads)
- **Storage:** Minimum 20GB available disk space

### Python Dependencies

All required Python packages are specified in `requirements.txt`. Core dependencies include:

- **pyspark**: Distributed data processing framework
- **kafka-python**: Kafka client library
- **psycopg2-binary**: PostgreSQL database adapter
- **pandas**: Data manipulation and analysis
- **python-dotenv**: Environment variable management

## Installation and Deployment

### Environment Setup

1. **Clone Repository**
```bash
git clone https://github.com/robert25893/de-coaching-lab.git
cd de-coaching-lab/99-project
```

2. **Create Python Virtual Environment**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
```

3. **Install Dependencies**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. **Configure Environment Variables**

Create and configure `.env` file with the following parameters:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=analytics_warehouse
DB_USER=your_username
DB_PASSWORD=your_password
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=product_events
```

## Operational Procedures

### Containerized Deployment

For production deployment using Docker:

```bash
docker container stop finalprj || true && docker container rm finalprj || true && \
docker run -ti --name finalprj \
  --network=streaming-network \
  --env-file .env \
  -p 4040:4040 \
  -v ./:/spark \
  -v ./data:/data \
  -v /home/duyng/finalprj/data/processed/currency:/data/unigap \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -e PYSPARK_DRIVER_PYTHON=python \
  -e PYSPARK_PYTHON='./environment/bin/python' \
  -e PYTHONPATH=/spark \
  unigap/spark:3.5 bash -c "\
    cd /spark && \
    python -m venv pyspark_venv && \
    source pyspark_venv/bin/activate && \
    pip install -U -r requirements.txt && \
    spark-submit \
      --master spark://spark-spark-1:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      src/main.py
  "
```

## Business Intelligence Dashboard

### Dashboard Overview

The project includes an interactive business intelligence dashboard that provides real-time visualization and analysis of customer behavior on the Glamira platform. The dashboard serves as the primary interface for stakeholders to monitor key performance indicators, track customer engagement trends, and derive actionable insights from the data warehouse.

### Dashboard 


## Analytics Capabilities

### Business Intelligence Outputs

The system provides comprehensive analytical capabilities for understanding customer behavior on the Glamira platform, including:

**Product Performance Analytics**
- Top 10 jewelry products by view count with time-series analysis
- Product engagement metrics and conversion funnel analysis
- Popular customization combinations (diamond and alloy preferences)

**Geographic Analytics**
- Top 10 countries by customer engagement volume on Glamira platform
- Geographic distribution analysis with country-level aggregations
- Cross-border luxury jewelry shopping behavior patterns
- International market penetration insights

**Traffic Source Analysis**
- Top 5 referrer URLs by traffic volume
- Attribution modeling for traffic sources
- Campaign effectiveness measurement

**Regional Store Performance**
- Country-specific store performance comparisons
- Store ranking by view count within geographic regions
- Multi-dimensional regional analysis

**Product Engagement Patterns**
- Jewelry product view duration analytics by product and country
- Time-on-page metrics for product customization interfaces
- Cross-country luxury product preference analysis
- Diamond and alloy customization engagement patterns

**Technology Stack Analytics**
- Browser and operating system usage statistics
- Device category performance metrics
- Technical optimization insights

## Technical Specifications

### Performance Characteristics

- **Data Ingestion Rate:** Real-time streaming with sub-second latency
- **Processing Throughput:** Batch processing of millions of records per hour
- **Query Performance:** Optimized dimensional model for sub-second analytical queries
- **Scalability:** Horizontally scalable architecture supporting data growth

### Data Quality Framework

- **Validation Rules:** Schema validation at ingestion
- **Data Cleansing:** Automated data quality checks and transformations
- **Audit Trail:** Complete logging of all ETL operations
- **Monitoring:** Real-time pipeline health monitoring and alerting

## Professional Skills Demonstrated

This project showcases proficiency in the following areas:

- **Big Data Technologies:** Apache Kafka, Apache Spark, distributed computing
- **Data Engineering:** ETL pipeline development, data modeling, workflow orchestration
- **Database Design:** Dimensional modeling, star schema, query optimization
- **Programming:** Python, PySpark, SQL, Bash scripting
- **DevOps:** Docker containerization, environment management
- **Architecture:** System design, scalability patterns, best practices
- **Data Governance:** Logging, monitoring, data quality management

## Technical References

- [Apache Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL JDBC Driver Documentation](https://jdbc.postgresql.org/documentation/)
- [Python Package Management Best Practices](https://packaging.python.org/tutorials/managing-dependencies/)
- [Kimball Dimensional Modeling Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)

## License

This project is maintained for professional portfolio purposes and educational reference.

---

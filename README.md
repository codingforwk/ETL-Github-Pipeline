# 🚀 ETL GitHub Pipeline Project

This project is a fully containerized, modular end-to-end data engineering pipeline that ingests GitHub Archive data, transforms it using Apache Spark and Delta Lake, triggers an Azure Synapse pipeline, and models data using dbt — all orchestrated with Apache Airflow.

## 🔧 Stack

- **Airflow** - Orchestration  
- **Python** - ETL scripting  
- **Apache Spark + Delta Lake** - Data transformation  
- **Azure Data Lake Storage Gen2** - Data storage  
- **Azure Synapse** - Data processing pipeline  
- **dbt** - Data modeling  
- **Terraform** - Infra as Code (Azure setup)  
- **Docker** - Ingestion container  
- **VSCode on Azure VM** - Dev environment  

## 📁 Project Structure

```text
ETL-Github-Pipeline/
├── dags/                   # Airflow DAGs
├── ingestion/              # Dockerized data ingestion scripts
├── transforms/             # PySpark + Synapse trigger + dbt
│   ├── bronze_to_silver/   # Spark transformation scripts
│   └── silver_to_gold/     # Synapse trigger & dbt models
├── utils/                  # Spark session helpers
├── config/                 # YAML config files
├── infra/                  # Terraform Azure setup
├── tests/                  # Unit tests
├── airflow-env/            # Python virtual environment
├── requirements.txt        # Dependency list
└── README.md               # Documentation
```

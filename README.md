![image](https://github.com/user-attachments/assets/4a1b8b5b-1b8e-4363-b475-f9f85cdf3c64)


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
## ⚙️ Pipeline Flow
1. Data Ingestion  
   - Collects GitHub data via Python scripts  
   - Stores raw data as Parquet in Azure Data Lake (Bronze)  

2. Bronze to Silver Transformation  
   - Processes data using Spark transformations  
   - Outputs cleaned data in Parquet format (Silver)  

3. Azure Synapse Activation  
   - Triggers Synapse pipelines via Python API  
   - Handles large-scale data processing  

4. dbt Modeling Layer  
   - Creates analytical models from processed data  
   - Generates final Gold-tier datasets  

5. Airflow Orchestration  
   - Coordinates all pipeline stages  
   - Handles scheduling and error recovery  

## 🚦 Setup Instructions
1. Clone repository:
   ```
   git clone https://github.com/your-repo/ETL-Github-Pipeline  
  
3. Install dependencies:
   ```
   pip install -r requirements.txt  

5. Initialize infrastructure:
   ```
   cd infra  
   terraform init  
   terraform apply  

7. Configure environment:
   ```
   export AZURE_TENANT_ID=your_tenant_id  
   export SYNAPSE_WORKSPACE=your_workspace_name  

## 🎚 Execution Commands
- Start Airflow:
  ```
  airflow standalone  

- Run Spark job:
  ```
  spark-submit transforms/bronze_to_silver/transform.py  

- Execute dbt models:
  ```
  dbt run --project-dir transforms/silver_to_gold  

## ✅ Quality Assurance
- Run unit tests:
  ```
  pytest tests/  

- Validate data models:
  ```
  dbt test  

- Monitor pipeline:
  ```
  airflow dags list-runs  

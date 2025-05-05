import os
import logging
from pyspark.sql import SparkSession

def get_spark(app_name="ETL-Github-Pipeline", master="local[*]", extra_conf=None):
    builder = SparkSession.builder.appName(app_name).master(master)

    # Add Delta Lake, Hadoop Azure, and Azure Storage packages
    builder = builder.config(
    "spark.jars.packages",
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-azure:3.3.4,"
    "org.apache.hadoop:hadoop-azure-datalake:3.3.4,"
    "com.microsoft.azure:azure-storage:8.6.6"
    )


    # *** ADD THIS LINE to specify the Delta Lake Maven repo ***
    builder = builder.config(
        "spark.jars.repositories",
        "https://repo1.maven.org/maven2,https://maven.delta.io/repository/maven-public/"
    )

    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


    builder = builder.config(f"fs.azure.account.key.{os.getenv('storage_account')}.dfs.core.windows.net", os.getenv('account_key'))


    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    return spark

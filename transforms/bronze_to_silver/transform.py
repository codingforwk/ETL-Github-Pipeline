import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from utils.spark_utils import get_spark

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def read_bronze_data(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Reads raw JSON data from the bronze layer.

    Args:
        spark (SparkSession): Spark session.
        bronze_path (str): Path to bronze JSON files.

    Returns:
        DataFrame: Raw data DataFrame.

    Raises:
        Exception: If reading data fails.
    """
    try:
        logger.info(f"Reading bronze data from {bronze_path}")
        df = spark.read.json(bronze_path)
        record_count = df.count()
        logger.info(f"Read {record_count} records from bronze data")
        return df
    except Exception as e:
        logger.error(f"Error reading bronze data: {e}")
        raise

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforms raw DataFrame into cleaned silver DataFrame.

    Steps:
    - Selects relevant columns.
    - Renames nested columns to flat schema.
    - Converts 'created_at' string to timestamp.
    - Adds 'event_date' column for partitioning.

    Args:
        df (DataFrame): Raw input DataFrame.

    Returns:
        DataFrame: Transformed DataFrame.

    Raises:
        Exception: If transformation fails.
    """
    try:
        df_transformed = df.select(
            col("id"),
            col("type"),
            col("actor.login").alias("actor_login"),
            col("repo.name").alias("repo_name"),
            to_timestamp("created_at").alias("created_at_ts")
        ).withColumn("event_date", col("created_at_ts").cast("date"))

        transformed_count = df_transformed.count()
        logger.info(f"Transformed data contains {transformed_count} records")
        return df_transformed
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def write_silver_data(df: DataFrame, silver_path: str) -> None:
    """
    Writes transformed DataFrame to Delta Lake silver path partitioned by event_date.

    Args:
        df (DataFrame): Transformed DataFrame.
        silver_path (str): Output Delta Lake path.

    Raises:
        Exception: If writing data fails.
    """
    try:
        logger.info(f"Writing data to {silver_path} as Delta Lake partitioned by event_date")
        df.write.format("delta").mode("overwrite").partitionBy("event_date").save(silver_path)
        logger.info("Write completed successfully")
    except Exception as e:
        logger.error(f"Error writing silver data: {e}")
        raise

def transform_bronze_to_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> None:
    """
    Orchestrates the ETL process: reads bronze data, transforms it, and writes to silver layer.

    Args:
        spark (SparkSession): Spark session.
        bronze_path (str): Input path for bronze data.
        silver_path (str): Output path for silver data.
    """
    df_raw = read_bronze_data(spark, bronze_path)
    df_transformed = transform_data(df_raw)
    write_silver_data(df_transformed, silver_path)

def main():
    """
    Entry point for local testing.

    Initializes SparkSession, sets example paths, runs ETL, and stops Spark.
    """
    spark = get_spark(app_name="ETL-Github-BronzeToSilver")

    # Example paths (replace with your config or CLI args)
    bronze_path = "abfss://raw-events@githubdatastoreyoussef2.dfs.core.windows.net/year=2024"
    silver_path = "abfss://processed-events@githubdatastoreyoussef2.dfs.core.windows.net/"


    transform_bronze_to_silver(spark, bronze_path, silver_path)

    spark.stop()

if __name__ == "__main__":
    main()

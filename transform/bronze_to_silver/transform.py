import logging
from pyspark.sql import SparkSession
from typing import Tuple
from utils.spark_utils import create_spark_session
from utils.config_loader import load_config
from .validation import validate_with_gx

logger = logging.getLogger(__name__)

def read_bronze_data(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Read bronze data from bronze storage.
    """
    try:
        logger.info(f"Reading bronze data from {bronze_path}")
        return spark.read.json(bronze_path)

    except Exception as e:
        logger.error(f"Error reading bronze data: {e}")
        raise

def transform_raw_data(df: DataFrame) -> DataFrame:
    """ Transform raw data into a structured format. """
    try:
        logger.info("Transforming raw data")
        
        return raw_df.select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.to_timestamp("created_at","yyyy-MM-dd'T'HH:mm:ss'Z").alias("event_time"),
            F.col("actor.id").alias("actor_id"),
            F.col("repo.name").alias("repo_name")).withColumn("event_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")
            )
    except Exception as e:
        logger.error(f"Error transforming raw data: {str(e)}")
        raise

def write_silver_data(df: DataFrame, silver_path: str) -> None:
    """
    Write transform data to silver layer/storage.
    """
    try:
        logger.info(f"Writing silver data to {silver_path}")
        df.write.format("delta").mode("overwrite").save(silver_path)
    except Exception as e:
        logger.error(f"Error writing silver data: {str(e)}")
        raise

def main():
    """
    Main function to run the transformation pipeline.
    """
    config = load_config()
    spark = create_spark_session()

    try:
        raw_df = read_bronze_data(spark, config["paths"]["bronze_path"])
        silver_df = transform_raw_data(raw_df)

        # Validate the transformed data
        validate_with_gx(silver_df, config["great_expectations"]["suite_name"])

        write_silver_data(silver_df, config["paths"]["silver_path"])

        logger.info("Transformation pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Error in transformation pipeline: {str(e)}")
        raise
    finally:
        spark.stop()
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
    
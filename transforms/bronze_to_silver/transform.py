import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, trim, lower
from utils.spark_utils import get_spark
from validations import validate_bronze, validate_silver  

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def read_bronze_data(spark: SparkSession, bronze_path: str) -> DataFrame:
    try:
        logger.info(f"Reading bronze data from {bronze_path}")
        df = spark.read.json(bronze_path)
        record_count = df.count()
        logger.info(f"Read {record_count} records from bronze data")

        logger.info("Validating bronze data quality...")
        if not validate_bronze(df):
            raise ValueError("Bronze data validation failed!")
        logger.info("Bronze data validation passed.")

        return df
    except Exception as e:
        logger.error(f"Error reading or validating bronze data: {e}")
        raise

def transform_data(df: DataFrame) -> DataFrame:
    try:
        df_transformed = df.select(
            col("id"),
            col("type"),
            col("actor.login").alias("actor_login"),
            col("repo.name").alias("repo_name"),
            to_timestamp("created_at").alias("created_at_ts")
        ).withColumn("event_date", col("created_at_ts").cast("date"))

        df_transformed = df_transformed.dropDuplicates(["id"])

        df_transformed = df_transformed.filter(
            col("id").isNotNull() &
            col("type").isNotNull() &
            col("actor_login").isNotNull() &
            col("repo_name").isNotNull() &
            col("created_at_ts").isNotNull()
        )

        df_transformed = df_transformed.withColumn("actor_login", lower(trim(col("actor_login")))) \
                                       .withColumn("repo_name", lower(trim(col("repo_name"))))
        
        VALID_EVENT_TYPES = [
            "PushEvent",
            "PullRequestReviewEvent",
            "PullRequestReviewCommentEvent",
            "IssueCommentEvent",
            "IssuesEvent",
            "CreateEvent",
            "ReleaseEvent"
        ]
        
        df_transformed = df_transformed.filter(col("type").isin(VALID_EVENT_TYPES))

        transformed_count = df_transformed.count()
        logger.info(f"Transformed data contains {transformed_count} records")

        logger.info("Validating silver data quality...")
        if not validate_silver(df_transformed):
            raise ValueError("Silver data validation failed!")
        logger.info("Silver data validation passed.")

        return df_transformed
    except Exception as e:
        logger.error(f"Error transforming or validating data: {e}")
        raise

def write_silver_data(df: DataFrame, silver_path: str) -> None:
    try:
        logger.info(f"Writing data to {silver_path} as Parquet partitioned by event_date")
        df.write.format("parquet").mode("overwrite").partitionBy("event_date").save(silver_path)
        logger.info("Write completed successfully")
    except Exception as e:
        logger.error(f"Error writing silver data: {e}")
        raise

def transform_bronze_to_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> None:
    df_raw = read_bronze_data(spark, bronze_path)
    df_transformed = transform_data(df_raw)
    write_silver_data(df_transformed, silver_path)

def main():
    spark = get_spark(app_name="ETL-Github-BronzeToSilver")

    bronze_path = "abfss://raw-events@githubdatastoreyoussef2.dfs.core.windows.net/year=2024"
    silver_path = "abfss://processed-events@githubdatastoreyoussef2.dfs.core.windows.net/silver"

    transform_bronze_to_silver(spark, bronze_path, silver_path)

    spark.stop()

if __name__ == "__main__":
    main()

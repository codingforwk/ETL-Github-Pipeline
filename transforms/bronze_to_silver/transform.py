import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, trim, lower
from utils.spark_utils import get_spark
from validations import validate_bronze, validate_silver  # Import GE validation functions

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def read_bronze_data(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Reads raw JSON data from the bronze layer, performs initial data quality validation.

    Args:
        spark (SparkSession): Active Spark session.
        bronze_path (str): Path to the bronze JSON files.

    Returns:
        DataFrame: Raw DataFrame loaded from bronze layer.

    Raises:
        Exception: If reading data fails or bronze data validation fails.
    """
    try:
        logger.info(f"Reading bronze data from {bronze_path}")
        df = spark.read.json(bronze_path)
        record_count = df.count()
        logger.info(f"Read {record_count} records from bronze data")

        # Validate bronze data quality using Great Expectations
        logger.info("Validating bronze data quality...")
        if not validate_bronze(df):
            raise ValueError("Bronze data validation failed!")
        logger.info("Bronze data validation passed.")

        return df
    except Exception as e:
        logger.error(f"Error reading or validating bronze data: {e}")
        raise

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforms raw bronze DataFrame into cleaned silver DataFrame with additional cleaning and validation.

    Steps:
    - Select relevant columns and flatten nested fields.
    - Convert 'created_at' string to timestamp.
    - Add 'event_date' column for partitioning.
    - Remove duplicates and filter out rows with nulls in critical columns.
    - Standardize text fields by trimming whitespace and converting to lowercase.
    - Validate silver data quality using Great Expectations.

    Args:
        df (DataFrame): Raw bronze DataFrame.

    Returns:
        DataFrame: Cleaned and validated silver DataFrame.

    Raises:
        Exception: If transformation or silver data validation fails.
    """
    try:
        df_transformed = df.select(
            col("id"),
            col("type"),
            col("actor.login").alias("actor_login"),
            col("repo.name").alias("repo_name"),
            to_timestamp("created_at").alias("created_at_ts")
        ).withColumn("event_date", col("created_at_ts").cast("date"))

        # Remove duplicate events by 'id'
        df_transformed = df_transformed.dropDuplicates(["id"])

        # Filter out rows with nulls in critical columns
        df_transformed = df_transformed.filter(
            col("id").isNotNull() &
            col("type").isNotNull() &
            col("actor_login").isNotNull() &
            col("repo_name").isNotNull() &
            col("created_at_ts").isNotNull()
        )

        # Standardize text columns: trim whitespace and convert to lowercase
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

        # Validate silver data quality using Great Expectations
        logger.info("Validating silver data quality...")
        if not validate_silver(df_transformed):
            raise ValueError("Silver data validation failed!")
        logger.info("Silver data validation passed.")

        return df_transformed
    except Exception as e:
        logger.error(f"Error transforming or validating data: {e}")
        raise

def write_silver_data(df: DataFrame, silver_path: str) -> None:
    """
    Writes the cleaned and validated silver DataFrame to Delta Lake storage partitioned by event_date.

    Args:
        df (DataFrame): Silver DataFrame to write.
        silver_path (str): Destination path in Delta Lake.

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
    Orchestrates the ETL pipeline from bronze to silver layer.

    Steps:
    - Reads bronze data.
    - Validates bronze data quality.
    - Transforms and cleans data into silver format.
    - Validates silver data quality.
    - Writes silver data to Delta Lake.

    Args:
        spark (SparkSession): Active Spark session.
        bronze_path (str): Input path for bronze data.
        silver_path (str): Output path for silver data.
    """
    df_raw = read_bronze_data(spark, bronze_path)
    df_transformed = transform_data(df_raw)
    write_silver_data(df_transformed, silver_path)

def main():
    """
    Main entry point for local testing.

    Initializes Spark session, defines data paths, runs ETL pipeline, and stops Spark session.
    """
    spark = get_spark(app_name="ETL-Github-BronzeToSilver")

    # Replace these paths with your actual data locations or pass as CLI args
    bronze_path = "abfss://raw-events@githubdatastoreyoussef2.dfs.core.windows.net/year=2024"
    silver_path = "abfss://processed-events@githubdatastoreyoussef2.dfs.core.windows.net/"

    transform_bronze_to_silver(spark, bronze_path, silver_path)

    spark.stop()

if __name__ == "__main__":
    main()

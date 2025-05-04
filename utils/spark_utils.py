from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)
def create_spark_session(app_name: str = "BronzeToSilver") -> SparkSession:
    """Create a Spark session with Delta Lake support"""
    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.azure.account.auth.type.githubstoreyoussef2.df.core.windows.net", "OAuth") \
            .getOrCreate()
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise
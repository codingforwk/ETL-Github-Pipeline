import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

VALID_EVENT_TYPES = [
    "PushEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssueCommentEvent",
    "IssuesEvent",
    "CreateEvent",
    "ReleaseEvent"
]

def validate_bronze(df: DataFrame) -> bool:
    """
    Basic data quality checks on bronze (raw) DataFrame.

    Args:
        df (DataFrame): Bronze Spark DataFrame.

    Returns:
        bool: True if all validations pass, else AssertionError is raised.
    """
    required_columns = ["id", "type", "actor", "repo", "created_at"]
    logger.info("Starting bronze data validation...")
    for col_name in required_columns:
        logger.info(f"Checking column '{col_name}' existence and nulls...")
        assert col_name in df.columns, f"Missing required column: {col_name}"
        null_count = df.filter(df[col_name].isNull()).count()
        assert null_count == 0, f"Column '{col_name}' contains {null_count} null values"
        logger.info(f"Column '{col_name}' passed null check.")
    logger.info("Bronze data validation passed.")
    return True

def validate_silver(df: DataFrame) -> bool:
    """
    Data quality checks on silver (cleaned) DataFrame.

    Args:
        df (DataFrame): Silver Spark DataFrame.

    Returns:
        bool: True if all validations pass, else AssertionError is raised.
    """
    critical_columns = ["id", "type", "actor_login", "repo_name", "created_at_ts", "event_date"]
    logger.info("Starting silver data validation...")
    for col_name in critical_columns:
        logger.info(f"Checking column '{col_name}' existence and nulls...")
        assert col_name in df.columns, f"Missing required column: {col_name}"
        null_count = df.filter(df[col_name].isNull()).count()
        assert null_count == 0, f"Column '{col_name}' contains {null_count} null values"
        logger.info(f"Column '{col_name}' passed null check.")

    logger.info("Validating event types...")
    invalid_events_count = df.filter(~df.type.isin(VALID_EVENT_TYPES)).count()
    assert invalid_events_count == 0, f"Found {invalid_events_count} rows with invalid event types"
    logger.info("All event types are valid.")

    logger.info("Checking uniqueness of 'id' column...")
    total_count = df.count()
    distinct_count = df.select("id").distinct().count()
    assert total_count == distinct_count, f"'id' column contains duplicates: total={total_count}, distinct={distinct_count}"
    logger.info("'id' column is unique.")

    logger.info("Silver data validation passed.")
    return True

from great_expectations.dataset import SparkDFDataset
import logging

logger = logging.getLogger(__name__)

def validate_with_gx(df: DataFrame, suite_name: str) -> None:
    """Execute Great Expectations validation suite"""
    try:
        logger.info("Starting Great Expectations validation")
        
        ge_df = SparkDFDataset(df)
        
        # Column Existence
        ge_df.expect_column_to_exist("event_id")
        ge_df.expect_column_to_exist("event_date")
        
        # Value Validation
        ge_df.expect_column_values_to_not_be_null("event_id")
        ge_df.expect_column_values_to_match_regex(
            "event_date", 
            r"^\d{4}-\d{2}-\d{2}$"
        )
        
        # Event Type Validation
        valid_events = ["PushEvent", "PullRequestEvent", "IssueCommentEvent"]
        ge_df.expect_column_values_to_be_in_set("event_type", valid_events)
        
        result = ge_df.validate()
        if not result["success"]:
            logger.error("Great Expectations validation failed!")
            logger.error(result)
            raise ValueError("Data validation failed")
            
        logger.info("All Great Expectations validations passed")
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise
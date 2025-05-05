import pytest
from pyspark.sql import SparkSession, DataFrame
from transforms.bronze_to_silver import transform
from utils.spark_utils import create_spark_session

@pytest.fixture(scope="session")
def spark():
    return create_spark_session(app_name="TestSession")

def test_spark_session(spark):
    assert spark is not None
    assert spark.conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"

def test_transformation(spark):
    test_data = [
        {
            "id": "123", 
            "type": "PushEvent",
            "created_at": "2024-05-20T12:00:00Z",
            "actor": {"id": 456},
            "repo": {"name": "test/repo"}
        }
    ]
    
    test_df = spark.createDataFrame(test_data)
    transformed = transform.transform_raw_data(test_df)
    
    # Schema Assertions
    assert "event_id" in transformed.columns
    assert "event_date" in transformed.columns
    assert transformed.schema["event_time"].dataType == TimestampType()
    
    # Data Assertions
    assert transformed.count() == 1
    assert transformed.first().event_date == "2024-05-20"
# tests/test_spark_delta.py

from utils.spark_utils import get_spark

def test_spark_session():
    """
    Test that SparkSession with Delta support can be created without errors.
    """
    spark = get_spark(app_name="TestDelta")
    print("SparkSession created successfully with Delta support.")
    spark.stop()

if __name__ == "__main__":
    test_spark_session()

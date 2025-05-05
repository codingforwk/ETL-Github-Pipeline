from utils.spark_utils import get_spark

def test_adls_access():
    spark = get_spark(app_name="TestADLSAccess")

    # Test write
    silver_path = "abfss://processed-events@githubdatastoreyoussef2.dfs.core.windows.net/test_write/"
    df = spark.createDataFrame([(1, "test")], ["id", "value"])
    df.write.format("delta").mode("overwrite").save(silver_path)
    print(f"Successfully wrote test data to {silver_path}")

    # Test read
    df_read = spark.read.format("delta").load(silver_path)
    df_read.show()

    spark.stop()

if __name__ == "__main__":
    test_adls_access()

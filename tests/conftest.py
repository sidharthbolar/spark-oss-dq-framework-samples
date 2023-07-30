import pytest
from pyspark.sql import SparkSession
import pydeequ

@pytest.fixture(scope='session')
def spark():

    spark = SparkSession.builder \
      .master("local[1]") \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .appName("oss_data_quality_frameworks_compare") \
      .getOrCreate()

    yield spark
    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()


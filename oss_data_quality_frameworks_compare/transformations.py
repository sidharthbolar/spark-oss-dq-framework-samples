import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
import quinn
from typing import Callable
from sklearn.datasets import fetch_california_housing
import pandas as pd
import pydeequ
from oss_data_quality_frameworks_compare.dq_with_pydeequ import data_quality


def create_spark_session():
    spark = SparkSession.builder \
        .master("local[1]") \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .appName("oss_dq_application") \
        .getOrCreate()

    return spark


def create_sample_dataframe(spark) -> DataFrame:
    # Fetch Sample dataset from sklearn module
    sklearn_dataset = fetch_california_housing()
    pd_df = pd.DataFrame(data=sklearn_dataset.data,
                         columns=sklearn_dataset.feature_names)

    # initialise spark session
    input_df = spark.createDataFrame(pd_df)
    return input_df


def run_dq_check_pydeequ(spark):
    df = create_sample_dataframe(spark)
    result_df = data_quality.pydeequ_constraint_verification(spark, df)
    return result_df

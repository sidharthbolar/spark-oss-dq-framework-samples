from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from sklearn.datasets import fetch_california_housing
import pandas as pd
import pydeequ
from oss_data_quality_frameworks_compare.dq_with_pydeequ import data_quality as pydq_data_quality
from oss_data_quality_frameworks_compare.dq_with_soda_core import data_quality as sc_data_quality


def create_spark_session():
    spark = SparkSession.builder \
        .master("local[*]") \
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
    input_df.createOrReplaceTempView("sklearn_housing")
    return input_df


def run_dq_check_pydeequ(spark) -> DataFrame:
    df = create_sample_dataframe(spark)
    result_df = pydq_data_quality.pydeequ_constraint_verification(spark, df)
    return result_df


def run_dq_check_soda_core(spark) -> tuple[dict, int]:
    _ = create_sample_dataframe(spark)
    result_dict,exit_code = sc_data_quality \
        .soda_core_constraint_verification(spark=spark, df_name="sklearn_housing")
    return result_dict,exit_code

from oss_data_quality_frameworks_compare.transformations import create_sample_dataframe,create_spark_session



def test_sample_dataframe_is_created(spark):
    df = create_sample_dataframe(spark)
    df.printSchema()

    assert (df.count()==20640)
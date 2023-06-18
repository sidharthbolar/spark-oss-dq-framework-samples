from oss_data_quality_frameworks_compare.transformations import run_dq_check_pydeequ, create_sample_dataframe


def test_pydeequ_constraint_verification(spark):
    result_df = run_dq_check_pydeequ(spark)
    result_df.show(truncate=False)
    success_constraint = result_df.selectExpr("constraint").filter(result_df.constraint_status=="Success").collect()
    failure_constraint = result_df.selectExpr("constraint").filter(result_df.constraint_status=="Failure").collect()
    size_failure = len(failure_constraint)
    size_success = len(success_constraint)

    assert (size_failure==1)&(size_success==3)


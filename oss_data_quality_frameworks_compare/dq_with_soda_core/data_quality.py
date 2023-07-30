import logging
from typing import Dict, Tuple

from pyspark.sql import SparkSession
from soda.scan import Scan


def _initialise_soda_scan(df_name: str, spark: SparkSession) -> Scan:
    scan = Scan()
    scan.set_scan_definition_name(f"{df_name}")
    scan.add_spark_session(spark,data_source_name=f"{df_name}")
    scan.set_data_source_name(f"{df_name}")
    scan.add_sodacl_yaml_str(
        f"""
                  checks for sklearn_housing:
                    - row_count = 20640
                    - missing_count(MedInc) = 0
                    - duplicate_count(MedInc) = 0
                    - invalid_count(MedInc) = 0:
                        valid min: 0
                """
    )

    return scan


# TODO check if feasible
def soda_core_analyzer(spark, df):
    pass


# TODO check if feasible
def soda_core_profiler(spark, df):
    pass


# TODO check if feasible
def soda_core_constraint_suggestor():
    pass


def soda_core_constraint_verification(spark, df_name) -> tuple[dict, int]:
    logging.info(f"Paramters passed by Sid {df_name}")
    scan = _initialise_soda_scan(df_name=df_name, spark=spark)
    # Execute the scan
    ##################
    exit_code = scan.execute()


    # Inspect the scan result
    #########################
    results = scan.get_scan_results()
    print(scan.get_logs_text())

    return results,exit_code
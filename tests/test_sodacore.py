from pathlib import Path

from soda.scan import Scan

from oss_data_quality_frameworks_compare.transformations import run_dq_check_soda_core, create_sample_dataframe


def test_sodacore_constraint_verification(spark):
    result_dict,exit_code = run_dq_check_soda_core(spark)
    print(result_dict)
    print(exit_code)

    # _ = create_sample_dataframe(spark)
    # scan = Scan()
    # # scan.set_scan_definition_name("sample")
    # scan.add_spark_session(spark_session=spark, data_source_name="sklearn_housing")
    # scan.set_data_source_name("sklearn_housing")
    # # print(scan_definition)
    # scan.add_sodacl_yaml_str(
    #     f"""
    #           checks for sklearn_housing:
    #             - row_count = 20640
    #             - missing_count(MedInc) = 0
    #             - duplicate_count(MedInc) = 0
    #             - invalid_count(MedInc) = 0:
    #                 valid min: 0
    #         """
    # )
    # exit_code = scan.execute()
    # print(exit_code)
    # print(scan.scan_results)
    # print(scan.get_logs_text())
    assert (exit_code == 2)
    var = {'definitionName': 'sklearn_housing', 'defaultDataSource': 'sklearn_housing',
           'dataTimestamp': '2023-07-30T03:33:02+00:00', 'scanStartTimestamp': '2023-07-30T03:33:02+00:00',
           'scanEndTimestamp': '2023-07-30T03:33:04+00:00', 'hasErrors': False, 'hasWarnings': False,
           'hasFailures': True,
           'metrics': [
               {'identity': 'metric-sklearn_housing-sklearn_housing-sklearn_housing-row_count',
                'metricName': 'row_count',
                'value': 20640, 'dataSourceName': 'sklearn_housing'},
               {'identity': 'metric-sklearn_housing-sklearn_housing-sklearn_housing-MedInc-invalid_count-2df817db',
                'metricName': 'invalid_count', 'value': 0, 'dataSourceName': 'sklearn_housing'},
               {'identity': 'metric-sklearn_housing-sklearn_housing-sklearn_housing-MedInc-missing_count',
                'metricName': 'missing_count', 'value': 0, 'dataSourceName': 'sklearn_housing'},
               {'identity': 'metric-sklearn_housing-sklearn_housing-sklearn_housing-MedInc-duplicate_count',
                'metricName': 'duplicate_count', 'value': 3234, 'dataSourceName': 'sklearn_housing'}], 'checks': [
            {'identity': 'e8d82c09', 'name': 'row_count = 20640', 'type': 'generic',
             'definition': 'checks for sklearn_housing:\n  row_count = 20640', 'resourceAttributes': [],
             'location': {'filePath': 'sodacl_string.yml', 'line': 3, 'col': 23}, 'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'filter': None, 'column': None,
             'metrics': ['metric-sklearn_housing-sklearn_housing-sklearn_housing-row_count'], 'outcome': 'pass',
             'outcomeReasons': [], 'archetype': None},
            {'identity': '8c6444f0', 'name': 'missing_count(MedInc) = 0', 'type': 'generic',
             'definition': 'checks for sklearn_housing:\n  missing_count(MedInc) = 0', 'resourceAttributes': [],
             'location': {'filePath': 'sodacl_string.yml', 'line': 4, 'col': 23}, 'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'filter': None, 'column': 'MedInc',
             'metrics': ['metric-sklearn_housing-sklearn_housing-sklearn_housing-MedInc-missing_count'],
             'outcome': 'pass',
             'outcomeReasons': [], 'archetype': None},
            {'identity': '120d9b33', 'name': 'duplicate_count(MedInc) = 0', 'type': 'generic',
             'definition': 'checks for sklearn_housing:\n  duplicate_count(MedInc) = 0', 'resourceAttributes': [],
             'location': {'filePath': 'sodacl_string.yml', 'line': 5, 'col': 23}, 'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'filter': None, 'column': 'MedInc',
             'metrics': ['metric-sklearn_housing-sklearn_housing-sklearn_housing-MedInc-duplicate_count'],
             'outcome': 'fail', 'outcomeReasons': [], 'archetype': None},
            {'identity': '508302e4', 'name': 'invalid_count(MedInc) = 0', 'type': 'generic',
             'definition': 'checks for sklearn_housing:\n  - invalid_count(MedInc) = 0:\n      valid min: 0\n',
             'resourceAttributes': [], 'location': {'filePath': 'sodacl_string.yml', 'line': 6, 'col': 23},
             'dataSource': 'sklearn_housing', 'table': 'sklearn_housing', 'filter': None, 'column': 'MedInc',
             'metrics': ['metric-sklearn_housing-sklearn_housing-sklearn_housing-MedInc-invalid_count-2df817db'],
             'outcome': 'pass', 'outcomeReasons': [], 'archetype': None}], 'queries': [
            {'name': 'sklearn_housing.sklearn_housing.aggregation[0]', 'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'partition': None, 'column': None,
             'sql': 'SELECT \n  COUNT(*),\n  COUNT(CASE WHEN MedInc IS NULL THEN 1 END),\n  COUNT(CASE WHEN NOT (MedInc IS NULL) AND NOT (MedInc >= 0.0) THEN 1 END) \nFROM sklearn_housing',
             'exception': None, 'duration': '0:00:01.335030'},
            {'name': 'sklearn_housing.sklearn_housing.MedInc.duplicate_count', 'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'partition': None, 'column': 'MedInc',
             'sql': '\nWITH frequencies AS (\n    SELECT COUNT(*) AS frequency\n    FROM sklearn_housing\n    WHERE MedInc IS NOT NULL\n    GROUP BY MedInc)\nSELECT count(*)\nFROM frequencies\nWHERE frequency > 1',
             'exception': None, 'duration': '0:00:00.454107'},
            {'name': 'sklearn_housing.sklearn_housing.MedInc.duplicate_count.failing_sql',
             'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'partition': None, 'column': 'MedInc',
             'sql': '\nWITH frequencies AS (\n    SELECT MedInc\n    FROM sklearn_housing\n    WHERE MedInc IS NOT NULL\n    GROUP BY MedInc\n    HAVING count(*) > 1)\nSELECT main.*\nFROM sklearn_housing main\nJOIN frequencies ON main.MedInc = frequencies.MedInc\n',
             'exception': None, 'duration': None},
            {'name': 'sklearn_housing.sklearn_housing.MedInc.duplicate_count.passing_sql',
             'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'partition': None, 'column': 'MedInc',
             'sql': '\nWITH frequencies AS (\n    SELECT MedInc\n    FROM sklearn_housing\n    WHERE MedInc IS NOT NULL\n    GROUP BY MedInc\n    HAVING count(*) <= 1)\nSELECT main.*\nFROM sklearn_housing main\nJOIN frequencies ON main.MedInc = frequencies.MedInc\n',
             'exception': None, 'duration': None},
            {'name': 'sklearn_housing.sklearn_housing.MedInc.duplicate_count.failing_rows_sql_aggregated',
             'dataSource': 'sklearn_housing', 'table': 'sklearn_housing', 'partition': None, 'column': 'MedInc',
             'sql': '\nWITH frequencies AS (\n    SELECT MedInc, COUNT(*) AS frequency\n    FROM sklearn_housing\n    WHERE MedInc IS NOT NULL\n    GROUP BY MedInc)\nSELECT *\nFROM frequencies\nWHERE frequency > 1\nORDER BY frequency DESC\nLIMIT 100',
             'exception': None, 'duration': None},
            {'name': 'sklearn_housing.MedInc.failed_rows[duplicate_count]', 'dataSource': 'sklearn_housing',
             'table': 'sklearn_housing', 'partition': None, 'column': 'MedInc',
             'sql': '\nWITH frequencies AS (\n    SELECT MedInc\n    FROM sklearn_housing\n    WHERE MedInc IS NOT NULL\n    GROUP BY MedInc\n    HAVING count(*) > 1)\nSELECT main.*\nFROM sklearn_housing main\nJOIN frequencies ON main.MedInc = frequencies.MedInc\n\nLIMIT 100',
             'exception': None, 'duration': '0:00:00.398175'},
            {'name': 'sklearn_housing.sklearn_housing.duplicate_count[MedInc].failed_rows.aggregated',
             'dataSource': 'sklearn_housing', 'table': 'sklearn_housing', 'partition': None, 'column': None,
             'sql': '\nWITH frequencies AS (\n    SELECT MedInc, COUNT(*) AS frequency\n    FROM sklearn_housing\n    WHERE MedInc IS NOT NULL\n    GROUP BY MedInc)\nSELECT *\nFROM frequencies\nWHERE frequency > 1\nORDER BY frequency DESC\nLIMIT 100',
             'exception': None, 'duration': '0:00:00.163844'}], 'automatedMonitoringChecks': [], 'profiling': [],
           'metadata': [], 'logs': [
            {'level': 'INFO', 'message': 'Soda Core 3.0.41', 'timestamp': '2023-07-30T03:33:02+00:00', 'index': 1,
             'doc': None, 'location': None},
            {'level': 'INFO', 'message': 'Using DefaultSampler', 'timestamp': '2023-07-30T03:33:04+00:00', 'index': 2,
             'doc': None, 'location': None},
            {'level': 'INFO', 'message': 'Scan summary:', 'timestamp': '2023-07-30T03:33:04+00:00', 'index': 3,
             'doc': None,
             'location': None},
            {'level': 'INFO', 'message': '3/4 checks PASSED: ', 'timestamp': '2023-07-30T03:33:04+00:00', 'index': 4,
             'doc': None, 'location': None},
            {'level': 'INFO', 'message': '    sklearn_housing in sklearn_housing',
             'timestamp': '2023-07-30T03:33:04+00:00',
             'index': 5, 'doc': None, 'location': None},
            {'level': 'INFO', 'message': '      row_count = 20640 [PASSED]', 'timestamp': '2023-07-30T03:33:04+00:00',
             'index': 6, 'doc': None, 'location': None},
            {'level': 'INFO', 'message': '      missing_count(MedInc) = 0 [PASSED]',
             'timestamp': '2023-07-30T03:33:04+00:00', 'index': 7, 'doc': None, 'location': None},
            {'level': 'INFO', 'message': '      invalid_count(MedInc) = 0 [PASSED]',
             'timestamp': '2023-07-30T03:33:04+00:00', 'index': 8, 'doc': None, 'location': None},
            {'level': 'INFO', 'message': '1/4 checks FAILED: ', 'timestamp': '2023-07-30T03:33:04+00:00', 'index': 9,
             'doc': None, 'location': None},
            {'level': 'INFO', 'message': '    sklearn_housing in sklearn_housing',
             'timestamp': '2023-07-30T03:33:04+00:00',
             'index': 10, 'doc': None, 'location': None},
            {'level': 'INFO', 'message': '      duplicate_count(MedInc) = 0 [FAILED]',
             'timestamp': '2023-07-30T03:33:04+00:00', 'index': 11, 'doc': None, 'location': None},
            {'level': 'INFO', 'message': '        check_value: 3234', 'timestamp': '2023-07-30T03:33:04+00:00',
             'index': 12,
             'doc': None, 'location': None},
            {'level': 'INFO', 'message': 'Oops! 1 failures. 0 warnings. 0 errors. 3 pass.',
             'timestamp': '2023-07-30T03:33:04+00:00', 'index': 13, 'doc': None,
             'location': None}]}


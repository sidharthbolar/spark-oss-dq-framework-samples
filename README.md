# Project Name

## Overview
This project provides functions for performing data quality checks using both pydeequ and soda-core libraries on a sample dataset fetched from the sklearn module.

## Prerequisites
- Python (>=3.9.6)
- Poetry (>=1.1.0)
- PySpark (>=3.0.0)
- quinn (>=0.5.0)
- sklearn (>=0.0.post5)
- scikit-learn (>=1.2.2)
- pydeequ (>=1.0.1)
- soda-core (>=3.0.41)
- soda-core-spark-df (>=3.0.41)

## Installation
1. Make sure you have Python and Poetry installed on your system.
2. Clone the repository and navigate to the project root directory.
3. Run the following command to install the project dependencies:
   ```bash
   poetry install

## Functions

### pydeequ_analyzer
```python
def pydeequ_analyzer(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Perform data analysis using pydeequ's Analyzer.
    
    Parameters:
        spark (SparkSession): SparkSession object.
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Result of the data analysis.
    """
```

### pydeequ_profiler
```python
def pydeequ_profiler(spark: SparkSession, df: DataFrame) -> Dict[str, DataFrame]:
    """
    Perform column profiling using pydeequ's ColumnProfiler.
    
    Parameters:
        spark (SparkSession): SparkSession object.
        df (DataFrame): Input DataFrame.

    Returns:
        Dict[str, DataFrame]: Profiles for each column.
    """
```

### pydeequ_constraint_suggestor
```python
def pydeequ_constraint_suggestor(spark: SparkSession, df: DataFrame) -> Dict[str, DataFrame]:
    """
    Generate constraint suggestions using pydeequ's ConstraintSuggestionRunner.
    
    Parameters:
        spark (SparkSession): SparkSession object.
        df (DataFrame): Input DataFrame.

    Returns:
        Dict[str, DataFrame]: Constraint suggestions for each column.
    """
```

### pydeequ_constraint_verification
```python
def pydeequ_constraint_verification(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Verify constraints using pydeequ's VerificationSuite.
    
    Parameters:
        spark (SparkSession): SparkSession object.
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Verification results.
    """
```

### soda_core_constraint_verification
```python
def soda_core_constraint_verification(spark: SparkSession, df_name: str) -> Tuple[Dict[str, DataFrame], int]:
    """
    Verify constraints using soda-core's Scan.
    
    Parameters:
        spark (SparkSession): SparkSession object.
        df_name (str): Name of the DataFrame to be scanned.

    Returns:
        Tuple[Dict[str, DataFrame], int]: Scan results and exit code.
    """
```

## How to Use
1. Create a SparkSession using `create_spark_session()` function.
2. Fetch the sample DataFrame using `create_sample_dataframe(spark)` function.
3. Perform data quality checks using either `run_dq_check_pydeequ(spark)` for pydeequ or `run_dq_check_soda_core(spark)` for soda-core.

## Example
```python
from my_project import create_spark_session, create_sample_dataframe, run_dq_check_pydeequ, run_dq_check_soda_core

spark = create_spark_session()
sample_df = create_sample_dataframe(spark)

# Perform data quality checks using pydeequ
pydeequ_result = run_dq_check_pydeequ(spark)

# Perform data quality checks using soda-core
soda_core_result, exit_code = run_dq_check_soda_core(spark)

print(pydeequ_result)
print(soda_core_result)
print(f"Exit Code: {exit_code}")
```

Feel free to modify and extend this project to suit your specific needs. Happy data quality checking!
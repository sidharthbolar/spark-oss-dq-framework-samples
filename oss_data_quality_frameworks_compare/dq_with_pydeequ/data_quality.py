from pydeequ import ColumnProfilerRunner
from pydeequ.analyzers import *
from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT
from pydeequ.checks import *
from pydeequ.verification import *


def pydeequ_analyzer(spark: SparkSession, df: DataFrame):
    analysisResult = AnalysisRunner(spark) \
        .onData(df) \
        .addAnalyzer(Size()) \
        .run()
    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)

    return analysisResult_df


def pydeequ_profiler(spark: SparkSession, df: DataFrame):
    result = ColumnProfilerRunner(spark) \
        .onData(df) \
        .run()
    return result.profiles


def pydeequ_constraint_suggestor(spark: SparkSession, df: DataFrame):
    suggestionResult = ConstraintSuggestionRunner(spark) \
        .onData(df) \
        .addConstraintRule(DEFAULT()) \
        .run()

    return suggestionResult


def pydeequ_constraint_verification(spark: SparkSession, df: DataFrame):
    check = Check(spark, CheckLevel.Warning, "Review Check")

    check_result = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
        check.hasSize(lambda x: x == 20640) \
            .isComplete("MedInc") \
            .isUnique("MedInc") \
            .isNonNegative("MedInc")) \
        .run()

    check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
    return check_result_df

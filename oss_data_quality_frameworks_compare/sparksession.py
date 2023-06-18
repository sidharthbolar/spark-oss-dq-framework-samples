from pyspark.sql import SparkSession

spark = (SparkSession.builder
            .master("local")
            .appName("oss_data_quality_frameworks_compare")
            .getOrCreate())

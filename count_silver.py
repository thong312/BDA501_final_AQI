from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CountSilver").getOrCreate()
print("TOTAL_SILVER_RECORDS=", spark.read.parquet("s3a://aq-lake/silver/measurements/").count())

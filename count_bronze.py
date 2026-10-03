from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
print('TOTAL_RECORDS=', spark.read.text('s3a://aq-lake/bronze/openaq/measurements/').count())
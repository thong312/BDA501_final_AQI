from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
print('TOTAL_RECORDS=', spark.read.json('s3a://aq-lake/bronze/openaq/measurements/').count())

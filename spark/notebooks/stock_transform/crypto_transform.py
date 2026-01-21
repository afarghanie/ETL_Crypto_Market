# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode, arrays_zip, from_unixtime, current_timestamp
from pyspark.sql.types import DateType

import os
import sys

if __name__ == '__main__':

    def app():
        # Create a SparkSession
        spark = SparkSession.builder.appName("TransformCrypto") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("fs.s3a.attempts.maximum", "1") \
            .config("fs.s3a.connection.establish.timeout", "5000") \
            .config("fs.s3a.connection.timeout", "10000") \
            .getOrCreate()

        # Read JSON from MinIO (multiLine=true to handle JSON arrays)
        df = spark.read.option("multiLine", "true").json(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}")

        # Flatten nested 'roi' struct into separate columns (CSV doesn't support nested objects)
        df_flattened = df.withColumn('roi_currency', df.roi.currency) \
            .withColumn('roi_percentage', df.roi.percentage) \
            .withColumn('roi_times', df.roi.times) \
            .drop('roi')

        # Add processing timestamp
        df_with_timestamp = df_flattened.withColumn('processed_at', current_timestamp())

        # Derive output filename from input
        input_path = os.getenv('SPARK_APPLICATION_ARGS')
        filename = os.path.basename(input_path)
        
        # Store in Minio
        df_with_timestamp.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(f"s3a://crypto-market/transformed_{filename}")
            
    app()
    os.system('kill %d' % os.getpid())
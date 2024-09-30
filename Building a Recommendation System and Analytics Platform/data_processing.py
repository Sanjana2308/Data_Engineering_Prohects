from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session for streaming
spark = SparkSession.builder \
    .appName("StreamingServiceAnalytics") \
    .getOrCreate()

# Read data from a streaming source (e.g., Kafka)
user_interaction_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "user_interactions") \
    .load()

# Process the stream: calculate total watch time per content in real-time
from pyspark.sql.functions import col, sum

# Assuming the schema includes 'content_id', 'watch_time', and 'interaction_type'
# Process the stream: calculate total watch time per content in real-time
watch_time_per_content = user_interaction_stream \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("split(value, ',') as data") \
    .select(col("data")[0].alias("content_id"),
            col("data")[1].alias("watch_time")) \
    .groupBy("content_id") \
    .agg(sum("watch_time").alias("total_watch_time"))

# Write the results to a console (for testing/debugging) or a database
query = watch_time_per_content \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Keep the streaming query active until manually stopped
query.awaitTermination()

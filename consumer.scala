import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val newsSchema = new StructType()
  .add("source", new StructType()
    .add("id", "string")
    .add("name", "string"))
  .add("author", "string")
  .add("title", "string")
  .add("description", "string")
  .add("url", "string")
  .add("urlToImage", "string")
  .add("publishedAt", "string")
  .add("content", "string")

// Read from Kafka
val dataFrame = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "news-topic")
  .option("failOnDataLoss", "false")
  .option("startingoffsets", "latest")
  .load()

// Extract JSON value from Kafka "value" column
val newsDF = dataFrame.selectExpr("CAST(value AS STRING) as json")
  .select(from_json(col("json"), newsSchema).as("data"))
  .select("data.*")

// Convert publishedAt to a proper timestamp
val newsWithTime = newsDF.withColumn("event_time", to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

// Aggregate: Group by 5-minute windows and source name
val aggregated = newsWithTime
  .withWatermark("event_time", "10 minutes")
  .groupBy(window(col("event_time"), "5 minutes"), col("source.name"))
  .count()

// Write to a file sink in append mode with a trigger every 70 seconds
val query = aggregated.writeStream
  .format("parquet")
  .option("path", "hdfs://bigdata-m/BigData/news-aggregates/")
  .option("checkpointLocation", "file:///home/elzeindanny5/checkpoints/news")
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("70 seconds"))
  .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("TicketProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("created_at", StringType()) \
    .add("demande", StringType()) \
    .add("type_demande", StringType()) \
    .add("priorite", StringType())

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "latest") \
    .load()

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_enriched = df.withColumn(
    "equipe_support",
    when(col("type_demande") == "Technique", "Support Technique")
    .when(col("type_demande") == "Facturation", "Support Facturation")
    .when(col("type_demande") == "Livraison", "Support Livraison")
    .when(col("type_demande") == "Compte", "Support Compte")
    .otherwise("Support Général")
)

query = df_enriched.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/app/data/output/tickets_agg_json") \
    .option("checkpointLocation", "/app/checkpoints/tickets_agg_json") \
    .start()

query.awaitTermination()
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_date, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Spark's Hadoop layer needs winutils on Windows
if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "financial-transactions"
OUTPUT_PATH = "s3a://warehouse/transactions"
CHECKPOINT_PATH = "s3a://warehouse/checkpoints/transactions"
USD_TO_EUR = 0.85

spark = (
    SparkSession.builder
    .appName("transactions-streaming")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

schema = StructType([
    StructField("id_transaction", StringType()),
    StructField("type_transaction", StringType()),
    StructField("montant", DoubleType()),
    StructField("devise", StringType()),
    StructField("date", StringType()),
    StructField("lieu", StringType()),
    StructField("moyen_paiement", StringType()),
    StructField("details", StructType([
        StructField("produit", StringType()),
        StructField("quantite", IntegerType()),
        StructField("prix_unitaire", DoubleType()),
    ])),
    StructField("utilisateur", StructType([
        StructField("id_utilisateur", StringType()),
        StructField("nom", StringType()),
        StructField("adresse", StringType()),
        StructField("email", StringType()),
    ])),
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

df_json = (
    df_kafka
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

df_clean = (
    df_json
    .withColumn("montant_eur", col("montant") * lit(USD_TO_EUR))
    .withColumn("timezone", lit("Europe/Paris"))
    .withColumn("date_ts", to_timestamp("date"))
    .withColumn("date_only", to_date("date_ts"))
    .filter(col("moyen_paiement") != "erreur")
    .filter(col("lieu").isNotNull())
)

query = (
    df_clean.writeStream
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("date_only")
    .outputMode("append")
    .start()
)
query.awaitTermination()

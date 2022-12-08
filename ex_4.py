from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "data/broadcast_logs"
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd"
)

# Ex. 4.3
logs_raw = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
)

# Ex. 4.4
logs_clean = logs.select(*[col for col in logs.columns if "ID" not in col])
logs_clean.printSchema()
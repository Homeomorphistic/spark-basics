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

logs_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True
).filter(F.col("PrimaryFG") == 1)

logs_and_channels = logs.alias("left").join(logs_identifier.alias("right"), on="LogServiceID", how="inner")

cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    header=True,
    inferSchema=True,
    sep="|"
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description")
)

cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
    header=True,
    sep="|",
    inferSchema=True
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_description")
)

full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(cd_program_class, "ProgramClassID", how="left")

(full_log
 .groupby("ProgramClassCD", "ProgramClass_description")
 .agg(F.sum("duration_seconds").alias("duration_total"), F.mean("duration_seconds").alias("duration_mean"))
 .orderBy("duration_total", ascending=False)
 .show(10, False)
 )

answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds")
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total")
    )
    .withColumn(
        "commercial_ratio",
        F.col("duration_commercial") / F.col("duration_total")
    )
)

answer.orderBy("commercial_ratio", ascending=False).show(10, False)

answer_no_null = answer.dropna(subset="commercial_ratio")
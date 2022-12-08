from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()
DIRECTORY = "data/broadcast_logs"

# Ex. 5.4
# left.join(right, how="left_anti", on="my_column").select("my_column").distinct()
# (left.alias("left").join(right.alias("right"), how="left", on="my_column")
#  .filter(F.col("right.my_column").isnull())
#  .select("my_column")
#  .distinct())

# Ex. 5.5
call_signs = spark.read.csv(
    os.path.join(DIRECTORY, "Call_Signs.csv"),
    sep=",",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd"
)

full_log.join(call_signs, on="LogIdentifierID")

# Ex. 5.6
prc_update = (
    answer
    .select(
        *answer.columns,
        F.when(
            F.col("LogIdentifierID") == "PRC",
            0.75
        ).otherwise(1.0)
        .alias("prc_multiplier")

    )

)
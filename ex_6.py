import pyspark.sql.types as T
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()
# Ex. 6.5
dict_schema = T.StructType([
    T.StructField("one", T.LongType()),
    T.StructField("one", T.ArrayType(T.LongType())),
])

# Ex. 6.6
DIRECTORY = "data/shows"
three_shows = spark.read.json(
    os.path.join(DIRECTORY, "shows-*.json"),
    multiLine=True
)

three_shows.select(
    "name",
    F.array_min("_embedded.episodes.airdate").cast("date").alias("first_episode"),
    F.array_max("_embedded.episodes.airdate").cast("date").alias("last_episode"),
).select(
    "first_episode",
    "last_episode",
    (F.col("last_episode") - F.col("first_episode")).alias("diff")
).show()

# Ex. 6.7
three_shows.select(
    "_embedded.episodes.airdate",
    "_embedded.episodes.name",
).show(3, False)

# Ex. 6.8
exo6_8 = spark.createDataFrame([[1, 2], [2, 4], [3, 9]], ["one", "square"])

sol6_8 = (
    exo6_8.groupby()
    .agg(
        F.collect_list("one").alias("one"),
        F.collect_list("square").alias("square"),
    )
    .select(F.map_from_arrays("one", "square"))
)
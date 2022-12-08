from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "data/shows"

shows = spark.read.json(
    os.path.join(DIRECTORY, "shows-silicon-valley.json")
)

three_shows = spark.read.json(
    os.path.join(DIRECTORY, "shows-*.json"),
    multiLine=True
)

array_subset = shows.select("name", "genres")

array_subset = array_subset.select(
    "name",
    array_subset.genres[0].alias("dot_and_index")
)

array_subset_repeated = array_subset.select(
    "name",
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    F.col("dot_and_index")
).select(
    "name",
    F.array("one", "two", "three").alias("Some_Genres"),
    F.array_repeat("dot_and_index", 5).alias("Repeated_Genres")
)

array_subset_repeated.show(1, False)

array_subset_repeated.select(
    "name", F.size("Some_Genres"), F.size("Repeated_Genres")
).show()

array_subset_repeated.select(
    "name",
    F.array_distinct("Some_Genres"),
    F.array_distinct("Repeated_Genres"),
).show(1, False)

columns = ["name", "language", "type"]

shows_map = shows.select(
    *[F.lit(column) for column in columns],
    F.array(*columns).alias("values")
)

shows_map = shows_map.select(F.array(*columns).alias("keys"), "values")

shows_map = shows_map.select(
    F.map_from_arrays("keys", "values").alias("mapped")
)

shows_map.select(
    F.col("mapped.name"),
    F.col("mapped")["name"],
    shows_map.mapped["name"],
).show()

shows_clean = shows.withColumn(
    "episodes", F.col("_embedded.episodes")
).drop("embedded")

episodes_name = shows_clean.select(F.col("episodes.name"))

import pyspark.sql.types as T

episode_links_schema = T.StructType(
    [
        T.StructField(
            "self", T.StructType([T.StructField("href", T.StringType())])
        )
    ]
)

episode_image_schema = T.StructType(
    [
        T.StructField("medium", T.StringType()),
        T.StructField("original", T.StringType()),
    ]
)

episode_schema = T.StructType(
    [
        T.StructField("_links", episode_links_schema),
        T.StructField("airdate", T.DateType()),
        T.StructField("airstamp", T.TimestampType()),
        T.StructField("airtime", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("image", episode_image_schema),
        T.StructField("name", T.StringType()),
        T.StructField("number", T.LongType()),
        T.StructField("runtime", T.LongType()),
        T.StructField("season", T.LongType()),
        T.StructField("summary", T.StringType()),
        T.StructField("url", T.StringType()),
    ]
)

embedded_schema = T.StructType(
    [
        T.StructField(
            "_embedded",
            T.StructType(
                [
                    T.StructField(
                        "episodes", T.ArrayType(episode_schema)
                    )
                ]
            ),
        )
    ]
)

shows_with_schema = spark.read.json(
    "./data/shows/shows-silicon-valley.json",
    schema=embedded_schema,
    mode="FAILFAST",
)

for column in ["airdate", "airstamp"]:
    shows.select(f"_embedded.episodes.{column}").select(
        F.explode(column)
    ).show(5)

import pprint

pprint.pprint(
    shows_with_schema.select(
        F.explode("_embedded.episodes").alias("episode")
    )
    .select("episode.airtime")
    .schema.jsonValue()
)

episodes = shows.select(
    "id", F.explode("_embedded.episodes").alias("episodes")
)
episodes.show(5, truncate=70)

episode_name_id = shows.select(
    F.map_from_arrays(
        F.col("_embedded.episodes.id"), F.col("_embedded.episodes.name")
    ).alias("name_id")
)

episode_name_id = episode_name_id.select(
    F.posexplode("name_id").alias("position", "id", "name")
)
episode_name_id.show(5)

collected = episodes.groupby("id").agg(
    F.collect_list("episodes").alias("episodes")
)

struct_ex = shows.select(
    F.struct(
        F.col("status"), F.col("weight"), F.lit(True).alias("has_watched")
    ).alias("info")
)
struct_ex.show(10, False)
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("Launching PySpark with custom options")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
).getOrCreate()

results = (
           spark.read.text("./data/gutenberg_books/*.txt")
.select(F.split(F.col("value"), " ").alias("line"))
.select(F.explode(F.col("line")).alias("word"))
.select(F.lower(F.col("word")).alias("word"))
.select(F.regexp_extract(F.col("word"), "[a-z']+", 0).alias("word"))
.where(F.col("word") != "")
.groupby(F.col("word"))
.count()
)

results.orderBy(F.col("count").desc()).show(10)
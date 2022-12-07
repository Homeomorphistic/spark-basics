from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (SparkSession
         .builder
         .appName("Analyzing the vocabulary of Pride and Prejudice.")
         .getOrCreate())

sc = spark.sparkContext

sc.setLogLevel("ERROR")
results = (
    spark.read.text("data/gutenberg_books/*.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word_lower"))
    .select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
    .orderBy(F.col("count"), ascending=False)
)

results.show()
#results.write.csv("data/simple_count.csv")
# or with only one file
#results.coalesce(1).write.csv("data/simple_count_single.csv")
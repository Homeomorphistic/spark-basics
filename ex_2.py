from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Ex. 2.2
exo2_2_df = spark.createDataFrame([("test", "more test", 10_000_000_000)], ["one", "two", "three"])
exo2_2_df.schema[2] != "string" # something like that

# Ex. 2.3
from pyspark.sql.functions import col, length

exo2_3_df = (spark.read.text("data/gutenberg_books/1342-0.txt")
             .select(length(col("value")))
             .withColumnRenamed("length(value)", "number_of_char")
             )

exo2_3_df = (spark.read.text("data/gutenberg_books/1342-0.txt")
             .select(length(col("value")).alias("number_of_char"))
             )

# Ex. 2.5
from pyspark.sql.functions import col, split, explode, lower, regexp_extract

book = spark.read.text("data/gutenberg_books/1342-0.txt")

lines = book.select(split(book.value, " ").alias("line"))

words = lines.select(explode(col("line")).alias("word"))

words_lower = words.select(lower(col("word")).alias("word_lower"))

words_clean = words_lower.select(
    regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word")
)

words_nonull = words_clean.where(col("word") != "")

words_without_is = words_nonull.where(col("word") != "is")
words_longer_than_3 = words_nonull.filter(length(col("word")) > 3)

# Ex. 2.6
words_in = words_nonull.filter(col("word").isin(["is", "not", "the", "if"]))
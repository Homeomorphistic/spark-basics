from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Analyzing the vocabulary of Pride and Prejudice.")
         .getOrCreate())

sc = spark.sparkContext

sc.setLogLevel("ERROR")

book = spark.read.text("data/gutenberg_books/1342-0.txt")
book.printSchema()
book.show()

from pyspark.sql.functions import split, col, explode, lower, regexp_extract
lines = book.select(split(book.value, " ").alias("line"))
words = lines.select(explode(col("line")).alias("word")).select(lower(col("word")).alias("word"))
words = words.select(regexp_extract(col("word"), "[a-z]+", 0).alias("word"))
words = words.filter(col("word") != "")

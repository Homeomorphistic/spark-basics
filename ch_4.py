from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# my_grocery_list = [
#     ["Banana", 2, 1.74],
#     ["Apple", 4, 2.04],
#     ["Carrot", 1, 1.09],
#     ["Cake", 1, 10.99],
# ]
#
# df_grocery_list = spark.createDataFrame(my_grocery_list, ["Item", "Quantity", "Price"])

import os
DIRECTORY = "data/broadcast_logs"
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd"
)

logs.printSchema()
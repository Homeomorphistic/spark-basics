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

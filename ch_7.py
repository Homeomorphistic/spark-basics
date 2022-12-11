from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

elements = spark.read.csv(
    "./data/elements/Periodic_Table_Of_Elements.csv",
    header=True,
    inferSchema=True,
)

elements.where(F.col("phase") == "liq").groupby("period").count().show()

elements.createOrReplaceTempView("elements")
try:
    spark.sql(
        """select period, count(*) from elements
            where phase='liq' group by period"""
    ).show(5)
except AnalysisException as e:
    print(e)

spark.catalog.dropTempView("elements")

from pyspark.sql import SparkSession
from pyspark.sql import  functions as F
#from lib.logger import Log4j
if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()
    #logger= Log4j(spark)
    df = spark.read.format("csv").option("header","true").load(r"C:\\Users\\chand\\PycharmProjects\\code\\*.csv")
    df.select(F.col("name").alias("emp_name"),F.col("age").alias("emp_age")).show()



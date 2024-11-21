from pyspark.sql import SparkSession
from pyspark.sql import  functions as F
#from lib.logger import Log4j
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()
#logger= Log4j(spark)
df = spark.read.format("csv").option("header","true").\
    load(r"gs://chandra75/toydata/retail.csv")
df.show()
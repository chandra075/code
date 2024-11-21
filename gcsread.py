from pyspark.sql import SparkSession
from pyspark.sql import  functions as F
#from lib.logger import Log4j
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()
#logger= Log4j(spark)
df1 = spark.read.format("csv").option("header","true").\
     load(r"gs://chandra75/toydata/retail.csv")

df1.show(1)
#Raading from BigQuery
#df2 = spark.read.format("bigquery")\
#.option("parentProject","woven-mesh-233413")\
#.option("project","woven-mesh-233413")\
#.option("table","woven-mesh-233413.gcp_etl.plit").load()
# Reading from gs:// and Writing a table in BQ in a new table if not there

df1.write.format("bigquery") \
    .option("parentProject", "woven-mesh-233413") \
    .option("project", "woven-mesh-233413") \
    .option("table", "woven-mesh-233413.etl.new") \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .mode("append") \
    .save()
print("Done")
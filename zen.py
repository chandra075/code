import sys
# Initialize the file paths
input_file_path = None
output_file_path = None
for i in range(1, len(sys.argv), 2):  # Check every other argument (i.e., flag and value)
    if sys.argv[i] == '--inputfile':
        input_file_path = sys.argv[i + 1]
    elif sys.argv[i] == '--outputfile':
        output_file_path = sys.argv[i + 1]
from pyspark.sql import SparkSession
from pyspark.sql import  functions as F
#from lib.logger import Log4j

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()
    #logger= Log4j(spark)
EX=[25]
df = spark.read.format("csv").option("header","true").load(input_file_path)
df.select(F.col("name").alias("emp_name"),F.col("age").alias("emp_age")).show()
new_df=df.filter(~df.age.isin(EX)).select(F.col("name"), F.col("age").alias("Age"))
new_df.write.format("csv").option("header", "true").mode("overwrite").save(output_file_path)



from pyspark.sql import SparkSession
from pyspark.sql import  functions as F
#from lib.logger import Log4j
if __name__ == '__main__':
    spark= SparkSession.builder\
    .appName("test")\
    .master("local[2]")\
    .getOrCreate()
    #logger= Log4j(spark)
    data_list =[("Unnat",14),
                ("Unnika",6),
                ("Trishika",1)]
    df = spark.createDataFrame(data_list).toDF("name","age")
    df.show()

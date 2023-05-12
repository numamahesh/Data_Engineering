import logging

from pyspark.sql import *
from pyspark.sql.functions import col, to_date

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Stores") \
        .master("local[2]") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ALL")
path=r"C:\Users\dnaga\Documents\Work_Umamahesh\Data_Engineering\resources\Superstore_orders.csv"
df = spark.read.option("header",True).options(delimiter=',').csv(path)

#print(df.filter(df.customer_name == "Claire Gute").show(truncate=False))
#print(df.select('customer_name').where('a%'))
#df.filter(col("customer_name").like("_a_d%")).show()
#df.select('order_id','product_name').where(df.order_date.between('2018-12-01','2020-12-31')).show()
#df2=df.filter(to_date("order_date", "yyyy-MM-dd").between('2020-12-01','2020-12-31'))

#df2.show()

df2=df.filter(df.order_date.between('12-01-2020','12-31-2020'))
df2.show()
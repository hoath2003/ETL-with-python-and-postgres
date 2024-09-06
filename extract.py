##import required libraries
import pyspark

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "/home/huyhoa/spark/jars/postgresql-42.7.3.jar") \
   .getOrCreate()


##read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pineline") \
   .option("dbtable", "movies") \
   .option("user", "postgres") \
   .option("password", "postgres") \
   .option("driver", "org.postgresql.Driver") \
   .load()
   
##add code below
user_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pineline") \
   .option("dbtable", "users") \
   .option("user", "postgres") \
   .option("password", "postgres") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the users dataframe
print(user_df.show())
print(movies_df.show())





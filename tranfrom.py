##import required libraries
import pyspark.sql


##create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.driver.extraClassPath', "/home/huyhoa/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

##read movies table from db using spark
movies_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/etl_pineline") \
    .option("dbtable", "movies") \
    .option("user", "postgres") \
   .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

##read users table from db using spark
users_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/etl_pineline") \
    .option("dbtable", "users") \
    .option("user", "postgres") \
   .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

## transforming tables
avg_rating = users_df.groupBy("movie_id").mean("rating")


##join the movies_df and avg_ratings table on id
df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)


##print all the tables/dataframes
print(movies_df.show())
print(users_df.show())
print(avg_rating.show())
print(df.show())






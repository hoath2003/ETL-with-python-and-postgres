##import required libraries
import pyspark.sql

##create spark session
spark = pyspark.sql.SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config('spark.driver.extraClassPath', "/home/huyhoa/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

##read movies table from db using spark
def extract_movies_to_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pineline") \
        .option("dbtable", "movies") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return movies_df

##read users table from db using spark
def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pineline") \
        .option("dbtable", "users") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return users_df


def transform_avg_ratings(movies_df, users_df):
    ## transforming tables
    avg_rating = users_df.groupBy("movie_id").mean("rating")
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id )
    df = df.drop("movie_id")
    return df


##load transformed dataframe to the database
def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/etl_pineline"
    properties = {"user": "postgres",
                  "password": "postgres",
                  "driver": "org.postgresql.Driver"
                  }
    df.write.jdbc(url=url,
                  table = "avg_ratings",
                  mode = mode,
                  properties = properties)

if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    ratings_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(ratings_df)

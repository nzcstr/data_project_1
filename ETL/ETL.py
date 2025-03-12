import findspark
findspark.init()
from pyspark.sql import SparkSession
from nzcstr_tools.misc import spark_flat_column, spark_custom_key, spark_gen_intermediate_table


def main():
    # Define MongoDB Connection URI
    #todo: Create function to read configuration parameters from file
    DB_NAME = "netflix_db"
    COLLECTION_NAME = "shows"
    #mongo_uri = f"mongodb://localhost:27017/{DB_NAME}.{COLLECTION_NAME}" # Use this when hosting code locally
    mongo_uri = f"mongodb://mongodb:27017/{DB_NAME}.{COLLECTION_NAME}" # super pito

    # Define PGSQL Conection details
    #pg_url = "jdbc:postgresql://localhost:5432/netflix_show_recommendation" # Use this when hosting code locally
    pg_url = "jdbc:postgresql://postgres:5432/netflix_show_recommendation"
    postgres_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }



    # Create a PySpark session with MongoDB support
    # Add postgreSQL support too
    print(f"MONGO URI: {mongo_uri}")
    spark = SparkSession.builder \
        .appName("ShowRecommendation") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,"
                "org.postgresql:postgresql:42.7.4") \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .getOrCreate()


    # Load Data from MongoDB
    df = (spark.read.format("mongodb").
          option("uri", mongo_uri).
          option("database", DB_NAME).
          option("collection", COLLECTION_NAME).
          load())

    df.printSchema()

    unparsed_null = '{"$numberDouble": "NaN"}'
    df = df.na.replace(unparsed_null, None) # More efficient code. Replaces all unparsed NULL values in all columns. No hard-coded

    # Get titles with director == NULL
    missing_directors = df.where(df["director"].isNull()).select("title", "director")
    missing_cast = df.where(df["cast"].isNull()).select("title", "cast")

    print(f"Number of entries BEFORE dropping NULL: {df.count()}")
    df_clean = df.dropna()
    print(f"Number of entries AFTER dropping NULL: {df_clean.count()}")


    # Convert "date_added" to DateType
    df_clean = df_clean.withColumn("date_added", df_clean["date_added"].cast('date'))
    ## Normalize data
    # T1 = shows
    # T2 = directors
    # T3 = show_directors (relational table)
    # T4 = casting
    # T5 = show_casting (relational table)
    # T6 = listed_in
    # T7 = show_listed (relational table)
    # T8 = countries
    # T9 = show_countries (relational table)
    tb_to_export = {}

    df_shows = df_clean.select(["show_id", "type", "title", "release_year", "rating", "duration", "description"])
    tb_to_export["shows"]=df_shows

    df_directors = df_clean.select("director")
    df_casting = df_clean.select("cast")
    df_genres = df_clean.select("listed_in")
    df_countries = df_clean.select("country")

    # Flattening data (and remove duplicates)
    df_directors = spark_flat_column(
        df=df_directors,
        in_col="director",
        out_col="director",
        sep=","
    )
    df_casting = spark_flat_column(df_casting, "cast", "actor", ",")
    df_genres = spark_flat_column(df_genres, "listed_in", "genres", ",")
    df_countries = spark_flat_column(df_countries, "country", "country", ",")


    # Create indexes
    df_directors = spark_custom_key(
        df=df_directors,
        target_col="director",
        idx_name="director_id",
        prefix="dt",
        pad=5
    )
    df_casting = spark_custom_key(df_casting, "actor", "actor_id", "ac", 6)
    df_genres = spark_custom_key(df_genres, "genres", "genres_id", "ge", 3)
    df_countries = spark_custom_key(df_countries, "country", "country_id", "co", 3)

    tb_to_export.update({"directors":df_directors, "casting":df_casting, "genres":df_genres, "countries":df_countries})

    # Generate intermediate tables
    rt_show_directors = spark_flat_column(df_clean, "director", "director_flat", ",").select("show_id", "director_flat")
    rt_show_directors = spark_gen_intermediate_table(
        left_df=rt_show_directors,
        right_df=df_directors,
        left_idx="show_id",
        right_idx="director_id",
        on_field_left="director_flat",
        on_field_right="director",
    )
    rt_show_casting = spark_flat_column(df_clean, "cast", "cast_flat", ",").select("show_id", "cast_flat")
    rt_show_casting = spark_gen_intermediate_table(rt_show_casting, df_casting, "show_id", "actor_id", "cast_flat", "actor")

    rt_show_genres = spark_flat_column(df_clean, "listed_in", "genres_flat", ",").select("show_id", "genres_flat")
    rt_show_genres = spark_gen_intermediate_table(rt_show_genres, df_genres, "show_id", "genres_id", "genres_flat", "genres")

    rt_show_countries = spark_flat_column(df_clean, "country", "country_flat", ",").select("show_id", "country_flat")
    rt_show_countries = spark_gen_intermediate_table(rt_show_countries, df_countries, "show_id", "country_id", "country_flat", "country")

    tb_to_export.update({"rt_show_directors":rt_show_directors, "rt_show_casting":rt_show_casting, "rt_show_genres":rt_show_genres, "rt_show_countries":rt_show_countries})

    # Export to postgresql DB

    for tb_name in tb_to_export:
        tb_to_export[tb_name].write.format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", tb_name) \
            .option("user", postgres_properties["user"]) \
            .option("password", postgres_properties["password"]) \
            .option("driver", postgres_properties["driver"]) \
            .mode("overwrite").save()
        print("Exported")



if __name__ == "__main__":
    main()
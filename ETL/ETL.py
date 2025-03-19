import findspark
findspark.init()
from pyspark.sql import SparkSession
from nzcstr_tools.misc import spark_flat_column,  spark_gen_intermediate_table, read_config, spark_remove_string_blank_spaces, export_table_to_postgres, spark_simple_custom_key
from pyspark.sql.functions import to_date, coalesce, col
from pyspark.sql.dataframe import DataFrame

def create_spark_session(app_name, mongo_uri):
    spark = SparkSession.builder \
        .appName("Netflix_shows") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,"
        "org.postgresql:postgresql:42.7.4") \
        .config("spark.mongodb.read.connection.uri",mongo_uri) \
        .config("spark.mongodb.write.connection.uri",
        mongo_uri) \
        .config("spark.driver.memory","4g") \
        .config("spark.sql.shuffle.partitions","4") \
        .getOrCreate()
    return spark

def read_from_mongo(spark, mongo_uri, db_name, collection_name):
    # Load Data from MongoDB
    df = (spark.read.format("mongodb").
          option("uri", mongo_uri).
          option("database", db_name).
          option("collection", collection_name).
          load())
    return df
def normalize_date_column(df):
    df = df.withColumn(
        "date_added_parsed",
        coalesce(
            to_date(col("date_added"),"MMMM dd, yyyy"),
                    to_date(col("date_added"),"MMMM d, yyyy")
        ))
    df = df.drop("date_added").withColumnRenamed("date_added_parsed", "date_added")
    return df



def main():
    local = False
    config_path = "config.json" if local else "./ETL/config.json"
    config = read_config(config_path)

    MONGO_DB_NAME = config["mongo_config"]["mongo_db_name"]
    COLLECTION_NAME = config["mongo_config"]["mongo_collection_name"]
    mongo_uri = ( f"mongodb://localhost:27017/{MONGO_DB_NAME}.{COLLECTION_NAME}"
                  if local else
        f"{config['mongo_config']['mongo_host']}{MONGO_DB_NAME}.{COLLECTION_NAME}")

    # Define PGSQL connection details
    if local:
        pg_uri = f"jdbc:postgresql://localhost:5432/{config['pg_config']['pg_db_name']}" # Use this when hosting code locally
    else:
     pg_uri = f"{config["pg_config"]["pg_host"]}{config['pg_config']['pg_db_name']}"

    postgres_properties = {
        "user": f"{config['pg_config']['pg_user']}",
        "password": f"{config['pg_config']['pg_user_password']}",
        "driver": f"{config['pg_config']['pg_driver']}"
    }

    # Create a PySpark session with MongoDB support

    spark = create_spark_session("Netflix_shows", mongo_uri)

    # Load Data from MongoDB
    df = read_from_mongo(spark, mongo_uri, MONGO_DB_NAME, COLLECTION_NAME)

    df.printSchema()

    unparsed_null = '{"$numberDouble": "NaN"}'
    df = df.na.replace(unparsed_null, None) # More efficient code. Replaces all unparsed NULL values in all columns. No hard-coded

    # Convert "date_added" to DateType
    spark.conf.set( # Necessary for correctly parsing data columns when exporting into postgreSQL
        "spark.sql.legacy.timeParserPolicy",
        "LEGACY")
    df_clean = normalize_date_column(df).dropna().cache()

    df_clean.select("date_added").show(5)
    df_clean.printSchema()

    ## Normalize data
    # T1 = shows
    # T2 = directors
    # T3 = show_directors (relational table)
    # T4 = casting
    # T5 = show_casting (relational table)
    # T6 = genres ("listed_in" in source db)
    # T7 = show_listed (relational table)
    # T8 = countries
    # T9 = show_countries (relational table)
    tb_to_export = {}
    print("##### PRINT 1")
    df_shows = df_clean.select(["show_id", "type", "title", "release_year", "date_added", "rating", "duration", "description"])
    df_shows = spark_remove_string_blank_spaces(df_shows)
    tb_to_export["shows"]=df_shows

    def process_dimension_table(base_df:DataFrame, dimension_col:str, id_name:str, id_prefix:str="", pad:int=5):
        out_df = base_df.select(dimension_col).dropna().dropDuplicates()
        out_df = spark_flat_column(out_df, dimension_col, dimension_col)
        out_df = spark_simple_custom_key(out_df, id_name, id_prefix, pad)
        out_df = spark_remove_string_blank_spaces(out_df)
        return out_df

    df_directors = process_dimension_table(df_clean, dimension_col="director", id_name="director_id", id_prefix="dt", pad=5)
    df_directors.printSchema()
    df_casting = process_dimension_table(df_clean, dimension_col="cast", id_name="actor_id" ,id_prefix="dt", pad=6).withColumnRenamed("cast", "actor")
    df_genres = process_dimension_table(df_clean, dimension_col="listed_in", id_name="genre_id", id_prefix="dt", pad=3).withColumnRenamed("listed_in", "genres")
    df_countries = process_dimension_table(df_clean, dimension_col="country",id_name="country_id" , id_prefix="dt", pad=3)


    # Generate intermediate tables + trim
    df_clean.printSchema()
    base = (df_shows.alias("s")
            .join(
                df_clean.alias("c").select(["show_id", "director", "cast", "listed_in", "country"]),
                col("s.show_id") == col("c.show_id"),
                how="inner")
            .select(
                col("s.show_id").alias("show_id"),
                col("c.cast").alias("cast"),
                col("c.listed_in").alias("listed_in"),
                col("c.country").alias("country"),
                col("c.director").alias("director")
    )).cache()
    base.show(5)
    df_clean.unpersist()

    # Export tables as soon as data is ready and no longer required.
    # export_table_to_postgres includes a check on dataframe to "unpersist" it if cached.
    export_table_to_postgres(df_shows, "shows", pg_uri, postgres_properties)
    rt_show_directors = spark_flat_column(base.select(["show_id", "director"]), "director", "director_flat", ",").select("show_id", "director_flat")
    rt_show_directors = spark_gen_intermediate_table(left_df=rt_show_directors, right_df=df_directors, left_idx="show_id", right_idx="director_id", on_field_left="director_flat", on_field_right="director", how="inner")
    export_table_to_postgres(df_directors, "directors", pg_uri, pg_properties=postgres_properties)
    export_table_to_postgres(rt_show_directors, "rt_show_directors", pg_uri, pg_properties=postgres_properties)


    rt_show_casting = spark_flat_column(base.select(["show_id", "cast"]), "cast", "cast_flat", ",").select("show_id", "cast_flat")
    rt_show_casting = spark_remove_string_blank_spaces(spark_gen_intermediate_table(rt_show_casting, df_casting, "show_id", "actor_id", "cast_flat", "actor"))
    export_table_to_postgres(df_casting, "casting", pg_uri, pg_properties=postgres_properties)
    export_table_to_postgres(rt_show_casting, "rt_show_casting", pg_uri, pg_properties=postgres_properties)

    rt_show_genres = spark_flat_column(base.select(["show_id", "listed_in"]), "listed_in", "genres_flat", ",").select("show_id", "genres_flat")
    rt_show_genres = spark_remove_string_blank_spaces(spark_gen_intermediate_table(rt_show_genres, df_genres, "show_id", "genre_id", "genres_flat", "genres"))
    export_table_to_postgres(df_genres, "genres", pg_uri, pg_properties=postgres_properties)
    export_table_to_postgres(rt_show_genres, "rt_show_genres", pg_uri, pg_properties=postgres_properties)


    rt_show_countries = spark_flat_column(base.select(["show_id", "country"]), "country", "country_flat", ",").select("show_id", "country_flat")
    rt_show_countries = spark_remove_string_blank_spaces(spark_gen_intermediate_table(rt_show_countries, df_countries, "show_id", "country_id", "country_flat", "country"))
    export_table_to_postgres(df_countries, "countries", pg_uri, pg_properties=postgres_properties)
    export_table_to_postgres(rt_show_countries, "rt_show_countries", pg_uri, pg_properties=postgres_properties)


    base.unpersist()



    spark.stop()

if __name__ == "__main__":
    main()
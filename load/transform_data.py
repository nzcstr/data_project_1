import findspark
findspark.init()
#from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode , col, expr, when
#from pyspark.sql.types import StringType, NullType, IntegerType, DoubleType, FloatType


#def main():
# Define MongoDB Connection URI
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
table_name = "netflix_shows"


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
# .config("spark.mongodb.input.uri", mongo_uri) \
#     .config("spark.mongodb.output.uri", mongo_uri) \
    #  .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
print("before loading data")
# Load Data from MongoDB
df = spark.read.format("mongodb").option("uri", mongo_uri).option("database", DB_NAME).option("collection",
                                                                                    COLLECTION_NAME).load()

# Show Data
df.show(5)

# Number of entries
print(f"Number of entries: {df.count()}")

print("Schema before editing")
df.printSchema()

# Exploring data with SparkSQL
df.createOrReplaceTempView("df_temp")
spark.sql("select distinct director from df_temp").show()

#
# def number_double_to_nan(value):
#     if isinstance(value, dict) and "$numberDouble" in value:
#         return None
#     else:
#         return value
# #nan_udf = udf(number_double_to_nan, StringType())
# @udf
# def clean_udf_with_type(value, data_type):
#     return number_double_to_nan(value, data_type)
#
# #df_w_nan = df.select([nan_udf(col(c)) for c in df.columns])
# df_w_nan = df.select([
#     clean_udf_with_type(col(c), df.schema[c].dataType).cast(df.schema[c].dataType).alias(c)
#     for c in df.columns
# ])
# df_w_nan.select("director").show()

# rdd = df.rdd
# rdd2 = rdd.filter(lambda x: "numberDouble" not in x)
# rdd.first()

# df_filtered_1 = df.where(~col("director").contains("numberDouble"))
# df_filtered_1.show()

unparsed_null = '{"$numberDouble": "NaN"}'
# df = df.replace(df.select("director").collect()[1][0], None) # Inefficient and risky hard-code. Select a specific cell containing an unparsed instance of NULL value then Replace all these instances in the "director" column
df = df.na.replace(unparsed_null, None) # More efficient code. Replaces all unparsed NULL values in all columns. No hard-coded

# Get titles with director == NULL
missing_directors = df.where(df["director"].isNull()).select("title", "director")
missing_cast = df.where(df["cast"].isNull()).select("title", "cast")


#cols = df.columns
# for c in cols:
#     df = df.withColumn(c, new_col_udf(c))
#     new_col_udf = udf(lambda x: None if "numberDouble" in x else x, df.schema[c].dataType)

# Get only without missing cast
#df_wo_null_cast = df.filter(df["cast"].isNotNull())
# Get only without missing directors
#df_wo_null_directors = df_wo_null_cast.filter(df_wo_null_cast["director"].isNotNull())
# Drop all rows with NULL values in any column
#Remove rows with NULL columns
df_wo_null_cast_directors = df.dropna()
print(f"Number of entries after dropping NULL: {df_wo_null_cast_directors.count()}")
# df_clean = df_wo_null_cast_directors.withColumn("")

#todo: convert dates to date datatype

## Normalize data
# T1 = shows
# T2 = directors
# T3 = show_directors (relational table)
# T4 = casting
# T5 = show_casting

df_shows = df_wo_null_cast_directors.select(["show_id", "type", "title"])

# Export to postgresql DB
df_wo_null_cast_directors.write.format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", table_name) \
    .option("user", postgres_properties["user"]) \
    .option("password", postgres_properties["password"]) \
    .option("driver", postgres_properties["driver"]) \
    .mode("overwrite").save()
print("Exported")

#    .option("driver", postgres_properties["driver"]) \

# df_flat = df.withColumn("cast_member", explode("cast"))
# df.select("director").show()
# df.select("cast").show()
#df2 = df.na.drop()







# Export to pgsql

# if __name__ == "__main__":
#     main()
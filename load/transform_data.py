#from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode , col, expr, when
#from pyspark.sql.types import StringType, NullType, IntegerType, DoubleType, FloatType


#def main():
# Define MongoDB Connection URI
DB_NAME = "netflix_db"
COLLECTION_NAME = "shows"
mongo_uri = f"mongodb://localhost:27017/{DB_NAME}.{COLLECTION_NAME}"



# Create a PySpark session with MongoDB support
spark = SparkSession.builder \
    .appName("ShowRecommendation") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .getOrCreate()

# Load Data from MongoDB
df = spark.read.format("mongodb").option("database", DB_NAME).option("collection", COLLECTION_NAME).load()

# Show Data
df.show(5)

# Number of entries
print(f"Number of entries: {df.count()}")

# Drop null values

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

df = df.replace(df.select("director").collect()[1][0], None)

# Get titles with director == NULL
missing_directors = df.where(df["director"].isNull()).select("title", "director")
missing_cast = df.where(df["cast"].isNull()).select("title", "cast")

print("Schema before")
df.printSchema()
cols = df.columns
# for c in cols:
#     df = df.withColumn(c, new_col_udf(c))
#     new_col_udf = udf(lambda x: None if "numberDouble" in x else x, df.schema[c].dataType)

df_w_null = df.filter(df["cast"].isNotNull())
# df_flat = df.withColumn("cast_member", explode("cast"))
# df.select("director").show()
# df.select("cast").show()
#df2 = df.na.drop()
print("Schema after")
df_w_null.printSchema()

#Remove rows with NULL columns
df_w_null = df_w_null.dropna()
# if __name__ == "__main__":
#     main()
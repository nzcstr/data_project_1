from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Define MongoDB Connection URI
mongo_uri = "mongodb://localhost:27017/movies_db.movies"
# conf = SparkConf("packages:org.mongodb.spark:mongo-spark-connector_2.12:10.4.1")
# sc = SparkContext(conf=conf)


# Create a PySpark session with MongoDB support
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .getOrCreate()

# Load Data from MongoDB
df = spark.read.format("mongodb").option("database", "movies_db").option("collection", "movies").load()

# Show Data
df.show(5)

from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_columns", 6)
import os

# Connect to MongoDB running in Docker
client = MongoClient("mongodb://localhost:27017/")

# Select db and collection
db = client["movies_db"]
collection = db["movies"]

src_csv = "./ml-latest/movies.csv"
df = pd.read_csv(src_csv)

# Convert dataframe to a list of dictionaries.
# Each entry is a single dictionary.
data = df.to_dict(orient="records")

# Insert data into MongoDB
collection.insert_many(data)

print("Data has been loaded into MongoDB!")


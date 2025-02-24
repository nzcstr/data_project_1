from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_columns", 6)
import wget
import os
from zipfile import ZipFile

# Download data
data_dir="./data"
if not os.path.isdir(data_dir):
    os.mkdir(data_dir)
url = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"
dst = os.path.join(data_dir, "ml-latest.zip")
wget.download(url, dst)
print("Data downloaded")
# Extract
with ZipFile(dst, 'r') as zip_file:
    zip_file.extractall(data_dir)
print(f"Data extracted into {data_dir}")

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


from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_columns", 6)
import wget
import os
from zipfile import ZipFile


def main():
# Download data
    DATA_DIR="./data"
    URL = "https://www.kaggle.com/api/v1/datasets/download/shivamb/netflix-shows"
    FILE_NAME = "netflix-shows.zip"
    DB_NAME = "netflix_db"
    COLLECTION_NAME = "shows"
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)

    dst = os.path.join(DATA_DIR, FILE_NAME)
    if not os.path.isfile(dst):
        wget.download(URL, dst)
        print("Data downloaded")
    else:
        print("Data already downloaded")

    #Extract
    if not os.path.isdir(os.path.join(DATA_DIR, FILE_NAME[:-4])):
        with ZipFile(dst, 'r') as zip_file:
            zip_file.extractall(DATA_DIR)
        print(f"Data extracted into {DATA_DIR}")
    else:
        print("Data already extracted")

    # Connect to MongoDB running in Docker
    client = MongoClient("mongodb://localhost:27017/")

    # Select db and collection
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    src_csv = os.path.join(DATA_DIR, FILE_NAME)
    df = pd.read_csv(src_csv)

    # Convert dataframe to a list of dictionaries.
    # Each entry is a single dictionary.
    data = df.to_dict(orient="records")

    # Insert data into MongoDB
    collection.insert_many(data)

    print("Data has been loaded into MongoDB!")

if __name__ == "__main__":
    main()
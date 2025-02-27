from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_columns", 6)
import wget
import os
from zipfile import ZipFile


def main():
# Download data
    data_dir="./data"
    url = "https://www.kaggle.com/api/v1/datasets/download/shivamb/netflix-shows"
    file_name = "netflix-shows.zip"
    db_name = "netflix_db"
    collection_name = "shows"
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    dst = os.path.join(data_dir, file_name)
    if not os.path.isfile(dst):
        wget.download(url, dst)
        print("Data downloaded")
    else:
        print("Data already downloaded")

    #Extract
    if not os.path.isdir(os.path.join(data_dir, file_name[:-4])):
        with ZipFile(dst, 'r') as zip_file:
            zip_file.extractall(data_dir)
        print(f"Data extracted into {data_dir}")
    else:
        print("Data already extracted")

    # Connect to MongoDB running in Docker
    client = MongoClient("mongodb://localhost:27017/")

    # Select db and collection
    db = client[db_name]
    collection = db[collection_name]

    src_csv = os.path.join(data_dir, file_name)
    df = pd.read_csv(src_csv)

    # Convert dataframe to a list of dictionaries.
    # Each entry is a single dictionary.
    data = df.to_dict(orient="records")

    # Insert data into MongoDB
    collection.insert_many(data)

    print("Data has been loaded into MongoDB!")

if __name__ == "__main__":
    main()
from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_columns", 6)
from nzcstr_tools.misc import download_file_url, extract_zip, csv_to_mongo
import os, glob


def main():
# Download data
    DATA_DIR="./data"
    URL = "https://www.kaggle.com/api/v1/datasets/download/shivamb/netflix-shows"
    DB_NAME = "netflix_db"
    COLLECTION_NAME = "shows"
    # MONGO_URI = "mongodb://localhost:27017"   # This works if hosting python code locally
    MONGO_URI = "mongodb://mongodb:27017" # This can be included as environment variable in docker-compose.yml
    path_to_file, file_name = download_file_url(DATA_DIR, URL)

    #Extract
    extracted_dir = extract_zip(path_to_file, DATA_DIR)

    #Export to MongoDB
    path_to_csv = glob.glob(os.path.join(extracted_dir, "*.csv"))[0] # There should be only one CSV file
    csv_to_mongo(path_to_csv, MONGO_URI, DB_NAME, COLLECTION_NAME)

    

if __name__ == "__main__":
    main()
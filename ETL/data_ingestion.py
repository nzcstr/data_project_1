import pandas as pd
pd.set_option("display.max_columns", 6)
from nzcstr_tools.misc import download_file_url, extract_zip, csv_to_mongo, read_config
import os, glob


def main():
    # Read config file

    config = read_config("./ETL/config.json")

    # Download data
    DATA_DIR = config["data_dir"]
    URL_KAGGLE = config["kaggle_url"]
    DB_NAME = config["mongo_config"]["mongo_db_name"]
    COLLECTION_NAME = config["mongo_config"]["mongo_collection_name"]
    # MONGO_URI = "mongodb://localhost:27017"   # This works if hosting python code locally
    MONGO_URI = config["mongo_config"]["mongo_host"] # This can be included as environment variable in docker-compose.yml

    path_to_file, file_name = download_file_url(DATA_DIR, URL_KAGGLE)

    #Extract
    extracted_dir = extract_zip(path_to_file, DATA_DIR)

    #Export to MongoDB
    path_to_csv = glob.glob(os.path.join(extracted_dir, "*.csv"))[0] # There should be only one CSV file
    csv_to_mongo(path_to_csv, MONGO_URI, DB_NAME, COLLECTION_NAME)

    

if __name__ == "__main__":
    main()
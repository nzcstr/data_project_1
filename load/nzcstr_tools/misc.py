import os
import wget
from zipfile import ZipFile
from pymongo import MongoClient
import pandas as pd


def kaggle_to_zip(file_path:str)-> str:
    # Files downloaded directly from kaggle have no extension
    # I simply add its extension, i.e. ".zip"
    new_file_name = file_path.split("/")[-1] + ".zip"
    return new_file_name

def download_file_url(dst_dir:str, url:str)-> tuple[str, str]:

    file_name = kaggle_to_zip(url)
    dst_file_path = os.path.join(dst_dir, file_name)

    if not os.path.isdir(dst_dir):
        os.mkdir(dst_dir)

    if not os.path.isfile(dst_file_path):
        wget.download(url, dst_file_path)
        print("Data downloaded")
    else:
        print("Data already downloaded")

    return dst_file_path, file_name

def extract_zip(zip_file_path:str, dst_dir:str) -> str:
    dst_folder_name = zip_file_path.split("/")[-1].split(".")[0]
    extracting_dir = os.path.join(dst_dir, dst_folder_name)
    if not os.path.isdir(extracting_dir):
        with ZipFile(zip_file_path, 'r') as zip_file:
            zip_file.extractall(extracting_dir)
            print("Data extracted")
    else:
        print("Data already extracted")
    return extracting_dir
def csv_to_mongo(csv_file_path:str, mongo_uri:str, mongo_db:str, mongo_collection:str) -> None:

    # Get MongoDB connection details
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]

    # Read data and format
    df = pd.read_csv(csv_file_path)
    data = df.to_dict(orient='records')

    # Export to mongo
    collection.insert_many(data)
    print("Data has been loaded into MongoDB!")
import os
import wget
from zipfile import ZipFile
from pymongo import MongoClient
import pandas as pd
from pyspark.sql.functions import split, explode, col, lpad, lit, row_number, concat
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame

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

def spark_flat_column(df:DataFrame, in_col:str, out_col:str, sep:str) -> DataFrame:
    ''' Flattens a field: generate multiple rows for a multi-value field.
        It yields a dataframe with removed duplicate rows.
    '''
    df_split = df.withColumn("array", split(col(in_col), sep))
    cols = df_split.columns[:-1]
    df_exploded = df_split.select(*cols, explode(col("array")).alias(out_col))
    df_exploded = df_exploded.dropDuplicates()
    return df_exploded

def spark_custom_index(df:DataFrame, target_col:str, idx_name:str, prefix:str, pad:int) -> DataFrame:

    # Add row number based on target col
    window_spec = Window.orderBy(target_col)
    out_nrow = df.withColumn("row_number", row_number().over(window_spec))

    # Customize index
    out_custom = df.withColumn(
        idx_name,
        concat(lit(prefix), lpad(col("row_number").cast("string"), pad, "0"))
    )

    return out_custom
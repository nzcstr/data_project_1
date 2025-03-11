import os
import wget
from zipfile import ZipFile
from pymongo import MongoClient
import pandas as pd
from pyspark.sql.functions import split, explode, col, lpad, lit, row_number, concat
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
import warnings

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

    # Check if collection exists already
    if collection.count_documents({}) > 0: # There is at least one document
        print(f"Data already exists in collection '{mongo_collection}'. Loading aborted!")
        return

    # Export to mongo
    if data:
        collection.insert_many(data)
        print("Data has been loaded into MongoDB!")
    else:
        print("No data has been detected in source CSV file!")

def spark_flat_column(df:DataFrame, in_col:str, out_col:str, sep:str) -> DataFrame:
    ''' Flattens a field: generate multiple rows for a multi-value field.
        It yields a dataframe with removed duplicate rows.
    '''
    if in_col != out_col:
        warnings.warn(f"New field name detected: {in_col} --> {out_col} ")
    df_split = df.withColumn("array", split(col(in_col), sep))
    cols = df_split.columns[:-1]

    if len(cols) == 1:
        # When working with a single field, return a dataframe with only the new field.
        # We assume that new field will replace the input field.
        df_exploded = df_split.select(explode(col("array")).alias(out_col))
        df_exploded = df_exploded.select(out_col)
    elif len(cols) > 1:
        if in_col == out_col:
            raise ValueError("Input and output columns must have different names")
        else:
            df_exploded = df_split.select(*cols, explode(col("array")).alias(out_col))

    df_exploded = df_exploded.dropDuplicates()

    return df_exploded

def spark_custom_key(df:DataFrame, target_col:str, idx_name:str, prefix:str, pad:int) -> DataFrame:

    # Add row number based on target col
    window_spec = Window.orderBy(target_col)
    out_nrow = df.withColumn("row_number", row_number().over(window_spec))

    # Customize index
    out_custom = out_nrow.withColumn(
        idx_name,
        concat(lit(prefix), lpad(col("row_number").cast("string"), pad, "0"))
    )

    out_custom = out_custom.drop("row_number")
    return out_custom

def spark_gen_intermediate_table(left_df:DataFrame, right_df:DataFrame,left_idx:str , right_idx:str, on_field_left:str, on_field_right:str=None) -> DataFrame:
    # left_df is a "flatten" Dataframe with only fields (left_idx, on_field_left)
    out_df = None
    if on_field_left == on_field_right:
        out_df =  left_df.join(right_df, on=on_field_left, how="left")
    elif on_field_left != on_field_right:
        out_df = left_df.join(right_df, left_df[on_field_left] == right_df[on_field_right], how="left")

    n_nulls = out_df.filter(col(right_idx).isNull()).count()
    if n_nulls > 0:
        warnings.warn(f"Join generated Null entries. Total nulls = {n_nulls}")

    out_df = out_df.select([left_idx, right_idx])
    return out_df



import numpy as np
import pandas as pd
from ast import literal_eval
from utils import get_full_class
from utils import load_config
import os
from datetime import datetime
    
def open_category_file(dataframe_path):
    df = pd.read_csv(dataframe_path)
    df = df[["id", "category"]]
    return df

def get_cate_by_id(dataframe, id):
    row = dataframe[dataframe["id"] == int(id)]
    categories = row["category"].values[0]
    return [x[1:-1] for x in categories[1:-1].split(" ")]

def iterate_through_user(datapath, dataframe):
    id = datapath.split("/")[-1].split(".")[0].split("-")[-1]
    categories = get_cate_by_id(dataframe, id)
    df = pd.read_csv(datapath)
    full_classes = get_full_class()
    columns = ["user_address", "user_name", "twitter_username", "twitter_name", *full_classes] 
    new_df = pd.DataFrame(columns = columns)
    df = df[["user_address", "user_name", "twitter_username", "twitter_name"]]
    result_df = pd.concat([df, new_df], axis = 0)
    result_df = result_df.fillna(0)
    for cate in categories:
        result_df[cate] = 1
    return result_df

def user_result(folder_path, dataframe):
    paths_list = os.listdir(folder_path)
    result_df = pd.DataFrame()
    for path in paths_list:
        try:
            new_df = iterate_through_user(folder_path + path, dataframe)
            result_df = pd.concat([result_df, new_df])
        except:
            continue
    group_by_cols = ["user_address", "user_name", "twitter_username", "twitter_name"]
    sum_cols = ["Art", "Cexes", "Derivatives", "Dexes", "Gaming", "Indexes", "Infrastructure", "Lending", "Memberships", 
            "PFPs", "Services", "Stablecoins", "Virtual Worlds", "Yield"]
    result_df.dropna(axis = 1, inplace = True)
    sums_per_category = result_df.groupby(group_by_cols)[sum_cols].sum().reset_index()
    return sums_per_category

def user_data():
    cfg = load_config("config/model.yaml")
    folder_path = "QuestN/data/users/{}/".format(datetime.utcnow().date())
    dataframe_path = cfg["save_file"]
    df = open_category_file(dataframe_path)
    user_result(folder_path, df).to_csv(cfg["result_paths"], index = False)
# def all_user_by_category
if __name__ == "__main__":
    dataframe_path = "testDataFrame.csv"
    folder_path = "QuestN/data/users/2023-12-21/"
    df = open_category_file(dataframe_path)
    user_result(folder_path, df).to_csv("save.csv", index = False)
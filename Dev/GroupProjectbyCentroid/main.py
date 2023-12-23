from get_categories import get_categories
from get_transactions import get_transaction_data
from pipeline import customPipeline
from pyspark.sql import SparkSession
import pandas as pd
from  merge_data import merge_data
from utils import append_df_to_db
from load_config import load_config, get_db_config

def train_cluster():
    transactions_df, project_smc_df = get_transaction_data(), get_categories(preprocess = True)
    merge_data(transactions_df, project_smc_df)
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("address_categories.csv")
    pipeline = customPipeline(category = ["Art",
               "Cexes",
               "Derivatives",
               "Dexes",
               "Gaming", 
               "Indexes",
               "Infrastructure",
               "Lending",
               "Memberships",
               "PFPs",
               "Services",
               "Stablecoins",
               "Virtual Worlds",
               "Yield"])
    pipeline.train(df)
    return pipeline
    
def infer_user_address(user_address,is_train = True):
    if is_train:
        pipeline = train_cluster()
    
    # spark = SparkSession.builder.appName("test").getOrCreate()
    # df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("address_categories.csv")
    # count_result = df.filter(col('userAddress') == user_address).count()
    # if count_result == 0:
    #     df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("save.csv")
    #     count_result = df.filter(col('userAddress') == user_address).count()
    #     if count_result == 0:
            
    #     else:
    res_df =  pipeline.infer(user_address)
    res_df.to_csv("res.csv", index=False)
    # config = get_db_config("users_clusters")
    append_df_to_db("users_clusters", res_df, "infer_results")
    return res_df


if __name__ == "__main__":
    res = infer_user_address("0x0012195f694f8125438c03c1692917e6de5fda76")
    
    # train()

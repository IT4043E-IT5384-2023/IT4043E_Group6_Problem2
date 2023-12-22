from get_categories import get_categories
from get_transactions import get_transaction_data
from pipeline import customPipeline
from pyspark.sql import SparkSession
import pandas as pd

def run():
    project_smc_df = get_categories(preprocess=True)
    transactions_df = get_transaction_data()
    # transaction_project_df = pd.merge(transactions_df,
    #                                 project_smc_df,
    #                                 left_on='to_address', 
    #                                 right_on='address', 
    #                                 how='inner')

    # address_categories = transaction_project_df.groupby(["mapped_categories", "from_address"]).count().reset_index()[['mapped_categories', 'from_address', '_id_x']]
    # address_categories.columns = ["categories", "userAddress", "count"]

    # for cate in address_categories.categories.unique():
    #     address_categories[f'{cate}_count'] = len(address_categories[address_categories.categories == cate])
        
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("address_categories.csv")
    pipeline = customPipeline()
    pipeline.train(df)
    
    pipeline.infer(df, 
                project_smc_df, 
                transactions_df)

if __name__ == "__main__":
    run()


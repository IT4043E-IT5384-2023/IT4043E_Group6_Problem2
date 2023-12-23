import difflib
import json
import time

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import tqdm
from pymongo import MongoClient
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.sql import SparkSession
from get_categories import get_categories
from get_transactions import get_transaction_data
import ast

# spark = SparkSession.builder.appName("test").getOrCreate()
# df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("address_categories.csv")
# df.printSchema()



class customPipeline:
    def __init__(self, category = None) -> None:
        self.category = category
    
    def train(self, df, save_path = None): 
        # feature_cols = df.columns
        # print(feature_cols)
        # feature_cols.remove("categories")
        # feature_cols.remove("user_address")
        project_smc_df = get_categories(preprocess=True)
        transactions_df = get_transaction_data()
        transaction_project_df = pd.merge(transactions_df,
                                          project_smc_df,
                                          left_on='to_address',
                                          right_on='address',
                                          how='inner')
        
        va = VectorAssembler(inputCols= self.category,
                            outputCol = "features")
        kmeans = KMeans()
        
        model = kmeans.fit(va.transform(df))
        pipeline_model = PipelineModel(stages = [va, model])

        # res = pipeline_model.transform(df).select("address", "prediction").toPandas()

        self.pipeline_model = pipeline_model
        if save_path:
            self.pipeline_model.write().overwrite().save(save_path)

        feature_cols = self.category
        
        model = self.pipeline_model.stages[-1]
        centroids = model.clusterCenters()

        distance_columns = [
            F.sqrt(sum((F.col(feature_cols[i]) - centroids[j][i]) ** 2 for i in range(len(feature_cols))))
            for j in range(len(centroids))
        ]

        # Add a column 'distance_to_centroid' to the DataFrame
        df_with_distance = self.pipeline_model.transform(df).withColumn(
            "distance_to_centroid",
            distance_columns[self.pipeline_model.transform(df).select("prediction").collect()[0]["prediction"]]
        )

        res_df = df_with_distance.select("user_address", "prediction", "distance_to_centroid").toPandas()
        # tmp.columns = ['user', 'centroids', 'distance']
        res_df.to_csv("train_tmp.csv", index=False)
        
        merged_df = pd.merge(res_df,
                             transaction_project_df[["from_address", "to_address"]],
                             left_on='user_address',
                             right_on='from_address',
                             how='inner')
        merged_df.drop("from_address", inplace = True, axis=1)
        print(merged_df.columns)
        merged_df.columns = ['user_address', 'prediction', 'distance_to_centroid','projectAddress']
        
        final_df = merged_df.groupby(["prediction"])[['projectAddress', 'distance_to_centroid']]\
            .agg({'projectAddress': list, 'distance_to_centroid': list}).reset_index()
            
        final_df = pd.merge(res_df[['user_address', 'prediction']], final_df,
                            left_on='prediction', right_on="prediction",
                            how="left")

        def limit_list_length(lst, max_length):
            return lst[:min(len(lst), max_length)]
        
        final_df['projectAddress'] = final_df['projectAddress'].apply(limit_list_length, max_length=5)
        final_df['distance_to_centroid'] = final_df['distance_to_centroid'].apply(limit_list_length, max_length=5)
        # final_df['distance_to_centroid'] = final_df['distance_to_centroid'].apply(lambda x: [value + np.random.rand() for value in ast.literal_eval(x)])

        final_df.to_csv("train_res.csv", index=False)
        print(merged_df.head())
        print(final_df.head())
        print(final_df.shape)

        
        
    def infer(self, user_address):
        project_smc_df = get_categories(preprocess=True)
        transactions_df = get_transaction_data()
        transaction_project_df = pd.merge(transactions_df,
                                          project_smc_df,
                                          left_on='to_address',
                                          right_on='address',
                                          how='inner')

        spark = SparkSession.builder.appName("test").getOrCreate()

        df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("address_categories.csv")
        count_result = df.filter(F.col('user_address') == user_address).count()
        if count_result == 0:
            print("Getting from questN")
            df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("save.csv")
            count_result = df.filter(F.col('user_address') == user_address).count()
            if count_result == 0:
                print("fallback")
                # transaction_project_df[["from_address", "to_address"]]
                value_counts = transaction_project_df.to_address.value_counts()


                # Get the most frequent item
                top_items = value_counts.head(10).index.tolist()

                return pd.DataFrame([user_address, -1, top_items, -1], 
                                    columns=['user_address', 'prediction', 'distance_to_centroid','projectAddress'])

        print("Getting from table")
        # df = df.filter(F.col('user_address') == user_address)
        feature_cols = self.category
        
        model = self.pipeline_model.stages[-1]
        centroids = model.clusterCenters()

        distance_columns = [
            F.sqrt(sum((F.col(feature_cols[i]) - centroids[j][i]) ** 2 for i in range(len(feature_cols))))
            for j in range(len(centroids))
        ]

        # Add a column 'distance_to_centroid' to the DataFrame
        df_with_distance = self.pipeline_model.transform(df).withColumn(
            "distance_to_centroid",
            distance_columns[self.pipeline_model.transform(df).select("prediction").collect()[0]["prediction"]]
        )

        res_df = df_with_distance.select("user_address", "prediction", "distance_to_centroid").toPandas()
        # tmp.columns = ['user', 'centroids', 'distance']
        # res_df.to_csv("tmp.csv", index=False)
        
        merged_df = pd.merge(res_df,
                             transaction_project_df[["from_address", "to_address"]],
                             left_on='user_address',
                             right_on='from_address',
                             how='right')
        merged_df.drop("from_address", inplace = True, axis=1)
        print(merged_df.columns)
        merged_df.columns = ['user_address', 'prediction', 'distance_to_centroid','projectAddress']
        
        final_df = merged_df.groupby(["prediction"])[['projectAddress', 'distance_to_centroid']]\
            .agg({'projectAddress': list, 'distance_to_centroid': list}).reset_index()
            
        final_df = pd.merge(res_df[['user_address', 'prediction']], final_df,
                            left_on='prediction', right_on="prediction",
                            how="left")

        def limit_list_length(lst):
            # sorted_values = sorted(zip(lst['distance_to_centroid'], lst['projectAddress']), reverse=True)[:5]

            return lst[:min(len(lst), 5)]
            # lst['distance_to_centroid'], lst['projectAddress'] = zip(*sorted_values)
            return lst
        
        final_df['projectAddress'] = final_df['projectAddress'].apply(limit_list_length)
        final_df['distance_to_centroid'] = final_df['distance_to_centroid'].apply(limit_list_length)
        # final_df['distance_to_centroid'] = final_df['distance_to_centroid'].apply(limit_list_length, axis=1)
        # final_df[['distance_to_centroid', 'projectAddress']]= final_df.apply(limit_list_length, axis=1)
        # final_df['distance_to_centroid'] = final_df['distance_to_centroid'].apply(lambda x: [value + np.random.rand() for value in ast.literal_eval(x)])
        final_df = final_df[final_df['user_address'] == user_address]
        print(merged_df.head())
        print(final_df.head())
        print(final_df.shape)

        # final_df.to_csv("cluster.csv", index=False)
        return final_df

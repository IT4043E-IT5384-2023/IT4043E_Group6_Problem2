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

# spark = SparkSession.builder.appName("test").getOrCreate()
# df = spark.read.options(inferSchema='True',delimiter=',', header='True').csv("address_categories.csv")
# df.printSchema()



class customPipeline:
    def __init__(self) -> None:
        pass
    
    def train(self, df): 
        feature_cols = df.columns
        print(feature_cols)
        feature_cols.remove("categories")
        feature_cols.remove("userAddress")
        
        va = VectorAssembler(inputCols= feature_cols,
                            outputCol = "features")
        kmeans = KMeans()
        
        model = kmeans.fit(va.transform(df))
        pipeline_model = PipelineModel(stages = [va, model])

        # res = pipeline_model.transform(df).select("address", "prediction").toPandas()

        self.pipeline_model = pipeline_model
        
        
    def infer(self, 
              df, 
              project_smc_df, 
              transactions_df):
        transaction_project_df = pd.merge(transactions_df,
                                          project_smc_df,
                                          left_on='to_address',
                                          right_on='address',
                                          how='inner')

        feature_cols = df.columns
        feature_cols.remove("categories")
        feature_cols.remove("userAddress")
        
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

        res_df = df_with_distance.select("userAddress", "prediction", "distance_to_centroid").toPandas()
        
        merged_df = pd.merge(res_df,
                             transaction_project_df[["from_address", "to_address"]],
                             left_on='userAddress',
                             right_on='from_address',
                             how='inner')
        merged_df.drop("from_address", inplace = True, axis=1)
        print(merged_df.columns)
        merged_df.columns = ['userAddress', 'prediction', 'distance_to_centroid','projectAddress']
        
        return merged_df.groupby("prediction")[['projectAddress', 'distance_to_centroid']].agg(list).reset_index()
        

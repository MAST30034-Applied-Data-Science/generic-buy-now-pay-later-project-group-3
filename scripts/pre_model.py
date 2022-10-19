import utils as u

import numpy as np
import pandas as pd
import seaborn as sns

from functools import reduce

from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, DataFrame

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.types import FloatType
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.ml.classification import MultilayerPerceptronClassifier, RandomForestClassifier
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Bucketizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator

class PreModel():
    """
    Class dedicated to building the data that leads to the model making
    """
    def __init__(self, dir = None):
        self.sp = (
            SparkSession.builder.appName("Process BNPL")
            .config("spark.sql.session.timeZone", "+11")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config('spark.sql.parquet.cacheMetadata', 'True')
            .getOrCreate()
        )
        self.dir = dir

    def __del__(self):
        self.sp.stop
        print("Initial model script completed!")

    def run_model(self):
        """
        Function to call and execute all functions related to the model
        """
        # Create X
        X = self.create_X()

        # Categorical transformations
        cat_transformer, X = self.process_category(self.drop_cols(X))
        self.write_model(cat_transformer, "category_transformer")

        # Quantile transformations
        X = self.process_quantiles(X)

        # Oversample undrrepresented populations
        X = self.populate_samples(X)
        print("Sampled and processed quantiles")

        # Write train test split into memory
        self.train_test_split(X)
        
    def create_X(self):
        """
        Function to combine and create a full X dataset
        """
        transactions = u.read_processed(self.sp, "transactions")
        merchants = u.read_processed(self.sp, "merchants")
        customers = u.read_processed(self.sp, "customers")
        c_fraud = u.read_curated(self.sp, "customer_fraud").withColumn("order_datetime", col("order_datetime").cast(DateType()))

        X = (
            transactions
            .join(c_fraud, on=["user_id", "order_datetime"])
            .join(merchants, on="merchant_abn")
            .join(customers, on="user_id")
        )
        u.write_data(X, "models", "X")
        return X

    def drop_cols(self, X: DataFrame):
        """
        Function to drop columns that are not required
        """
        return X.drop(
            "user_id",
            "merchant_abn",
            "order_datetime",
            "order_id",
            "name",
            "postcode",
            "holiday"
        )

    def process_category(self, data: DataFrame):
        """
        Function to process categorical data
        """
        categories = [
            "dayofmonth",
            "dayofweek",
            "month",
            "tags",
            "state",
            "gender",
            "Earnings_Class"
        ]

        # Pipeline
        indexers = [StringIndexer(inputCol=c, outputCol=c+"_index") for c in categories]
        encoders = [OneHotEncoder(inputCol=c+"_index", outputCol=c+"_encoded") for c in categories]
        transformer = Pipeline(stages=indexers + encoders).fit(data)
        transformed = transformer.transform(data)

        for c in categories:
            transformed = transformed.drop(c).drop(c+"_index")

        # Write category transformer to memory for later use
        print("Created Categories")
        return transformer, transformed

    def write_model(self, model: PipelineModel, model_name: str):
        """
        Function to write models into memory for later use
        """
        model.write().overwrite().save(u.safety_check("models") + "/" + model_name)
        # writer = model._call_java("write")
        # writer.save(u.safety_check("models") + model_name)
        print("Saved Categorical Model")

    def process_quantiles(self, data: DataFrame):
        """
        Create quantiles for the y variables (fraud detection)
        """
        return Bucketizer(
            splits=list(range(0, 101, 10)),
            inputCol="fraud_probability",
            outputCol="fraud_buckets"
        ).transform(data).drop("fraud_probability")

    def populate_samples(self, data: DataFrame):
        """
        Function to populate the data with more sample from underrepresented categories
        """
        fractions = [0, 0, 0, 2, 4, 7, 15, 20, 35, 250]

        return reduce(
            DataFrame.unionAll,
            [
                data.filter(
                    data.fraud_buckets == float(x)).sample(
                        withReplacement=True,
                        fraction=float(fractions[x]),
                        seed=69
                        ) for x in range(3, 10)
            ] + [
                data.filter(
                    data.fraud_buckets == float(x)) for x in range(0, 3)
            ]
        )

    def train_test_split(self, data: DataFrame):
        """
        Function returns the given dataset as a 70/20/10 split
        """
        train, val, test = data.randomSplit([0.7, 0.2, 0.1], seed=69)
        u.write_data(train, "models", "train_raw")
        u.write_data(val, "models", "val_raw")
        u.write_data(test, "models", "test_raw")
        print("Performed train test split")
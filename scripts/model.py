from unicodedata import category
import utils as u

import numpy as np
import pandas as pd
import seaborn as sns

from functools import reduce

from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, DataFrame

from pyspark.ml import Pipeline, PipelineModel, Transformer
from pyspark.sql.types import FloatType
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.ml.classification import MultilayerPerceptronClassifier, RandomForestClassifier
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Bucketizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator

class Model():
    """
    Class to that vectorizes and runs the model
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
        # Output arg which should point to the data folder
        self.dir = dir

        # Read model files
        self.train = u.read_models(self.sp, "train_raw")
        self.val = u.read_models(self.sp, "val_raw")
        self.test = u.read_models(self.sp, "test_raw")

    def __del__(self):
        self.sp.stop
        print("Finished building the model")

    def run_model(self):
        """
        Function to call all model related functions
        """
        # Process numerical data
        self.train = self.process_numerical(self.train)
        self.val = self.process_numerical(self.val)
        self.test = self.process_numerical(self.test)
        print("Processed Numerical")

        # Process vector data
        self.train = self.vectorize(self.train, "fraud_buckets")
        self.val = self.vectorize(self.val, "fraud_buckets")
        self.test = self.vectorize(self.test, "fraud_buckets")
        print("Vectorized data")

        # Write model data
        u.write_data(self.train, "models", "train_vector")
        u.write_data(self.val, "models", "val_vector")
        u.write_data(self.test, "models", "test_vector")
        
        self.predictions()
        print("Predicted full table")

    def process_numerical(self, data: DataFrame):
        """
        Function to scale numerical columns
        """
        columns = [
            'dollar_value', 'avg_monthly_inc', 'BNPL_Fee', 'monthly_entropy', 'postcode_entropy', 
            'revenue', 'Number of individuals lodging an income tax return', 'Average taxable income or loss', 
            'Median taxable income or loss', 'Proportion with salary or wages', 'Count salary or wages', 
            'Average salary or wages', 'Median salary or wages', 'Proportion with net rent', 'Count net rent', 
            'Average net rent', 'Median net rent', 'Average total income or loss', 'Median total income or loss', 
            'Average total deductions', 'Median total deductions', 'Proportion with total business income', 
            'Count total business income', 'Average total business income', 'Median total business income', 
            'Proportion with total business expenses', 'Count total business expenses', 
            'Average total business expenses', 'Median total business expenses', 'Proportion with net tax', 
            'Count net tax', 'Average net tax', 'Median net tax', 'Count super total accounts balance', 
            'Average super total accounts balance', 'Median super total accounts balance'
        ]

        va = VectorAssembler(inputCols=columns, outputCol="to_scale")
        sc = StandardScaler(inputCol="to_scale", outputCol="scaled")

        va_data = va.transform(data)
        data = sc.fit(va_data).transform(va_data)
        
        # Drop other columns
        for c in columns:
            data = data.drop(c)
        return data.drop("to_scale")

    def vectorize(self, data: DataFrame, outcome: str):
        """
        Function to vectorize all the processed data
        """
        data = data.withColumnRenamed(outcome, "label")
        return VectorAssembler(
            inputCols=[c for c in data.drop("label").columns],
            outputCol="features"
        ).transform(data)

    def vectorize_full(self, data: DataFrame, drop: list):
        """
        Function to vectorize all the processed data
        """
        return VectorAssembler(
            inputCols= [c for c in data.columns if not c in drop],
            outputCol="features"
        ).transform(data)

    def mlp(self):
        """
        Function that calls and runs all MLP related functions
        """
        model = self.create_mlp_model().fit(
            self.train.select("features", "label").dropna()
        )
        val_output = model.transform(self.val.dropna())
        test_output = model.transform(self.test.dropna())

    def create_mlp_model(self):
        """
        Returns defined MLP model with specified features
        """
        layers = [122, 256, 64, 10]
        return MultilayerPerceptronClassifier(
            labelCol='label',
            featuresCol='features',
            solver='gd',
            maxIter=100,
            layers=layers,
            blockSize=64,
            seed=69)

    def predictions(self):
        """
        Function that calls all functions related to the Random Forest Model
        """
        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=100
        )
        model = rf.fit(self.train)
        self.get_metrics(model)

        # Getting predictions for the entire dataset
        full = self.get_full_processed()
        print("Full data processed")

        full_pred = model.transform(
            full.drop("rawPrediction", "probability", "prediction") # If any
        ).drop("scaled", "features", "rawPrediction", "probability")
        u.write_data(full_pred, u.safety_check("models"), "random_forest_output_full")
         

    def get_accuracy(self, pred_df: DataFrame):
        """
        Function to get and print accuracy of model
        """
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        print(evaluator.evaluate(pred_df))

    def get_mse(self, pred_df: DataFrame):
        """
        Function to calculate the MSE
        """
        mse_score = pred_df.select(
            "label", "prediction"
            ).withColumn(
                "MSE",
                ((col("label") - col("prediction")) ** 2) ** 0.5
            )
        grouped_mse_score = mse_score.groupBy("MSE").count()
        print("The MSE is given by")
        grouped_mse_score.withColumn(
            "WMSE",
            col("MSE") * col("count")
            ).select("WMSE").groupBy().avg().show()

    def get_metrics(self, model: Transformer):
        """
        Function to get metrics of the val and test predictions 
        """
        # Predictions
        val_pred = model.transform(self.train)
        self.get_accuracy(val_pred)

        test_pred = model.transform(self.test)
        self.get_accuracy(test_pred)

        # MSE Metrics
        # self.get_mse(test_pred)

    def get_full_processed(self):
        """
        Full data that is processed
        """
        full = u.read_processed(
            self.sp,
            "transactions"
        ).join(
            u.read_processed(self.sp, "merchants"),
            on="merchant_abn"
        ).join(
            u.read_processed(self.sp, "customers"),
            on="user_id"
        ).drop("name", "order_id", "holiday")

        print("Read full model")

        full = self.create_categories(full)
        print("Completed Categorical")

        full = self.process_numerical(full)
        print("Comepleted Numerical")

        # Vectorize
        return self.vectorize_full(
            full,
            ["user_id", "merchant_abn", "order_datetime", "postcode"]
        ) 

    def create_categories(self, full: DataFrame):
        """
        Function to create and return categorical data
        """
        # Categorical model
        category_model = PipelineModel.load(u.safety_check("models") + "/category_transformer")
        full = category_model.transform(full)
        
        # Redefine categories to drop columns
        categories = [
            "dayofmonth",
            "dayofweek",
            "month",
            "tags",
            "state",
            "gender",
            "Earnings_Class"
        ]

        for c in categories:
            full = full.drop(c).drop(c+"_index")
        return full
import os
import numpy as np
import pandas as pd
from pyspark.sql.types import FloatType

import utils as u

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

class Model_Data():
    """
    Class to clean and output data that is used for the model
    """

    def __init__(self) -> None:
        self.sp = (
            SparkSession.builder.appName("Model")
            .config("spark.sql.session.timeZone", "+11")
            .getOrCreate()
        )
        self.transactions = u.read_processed(self.sp, "transactions")
        self.merchants = u.read_processed(self.sp, "merchants")
        self.customers = u.read_processed(self.sp, "customers")

        self.c_fraud = u.read_curated(self.sp, "customer_fraud")
        self.c_fraud = self.c_fraud.withColumn("order_datetime", col("order_datetime").cast(DateType()))

    def __del__(self):
        self.sp.stop
        print("Finished outputting model data")

    def create_categorical(self):
        """
        Function to create categorical variables from data
        """

    def output_data(self):
        """
        Function to check fraud data and take a subset of transactions for it
        """
        full_fraud = self.transactions.join(self.c_fraud, on=["user_id", "order_datetime"])
        full_fraud = full_fraud.join(self.merchants, on="merchant_abn").join(self.customers, on="user_id")

import os
import utils
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

class Process():
    """
    Class dedicated to processing, transforming, and processing the data
    """

    def __init__(self):
        self.sp = (
            SparkSession.builder.appName("Process BNPL")
            .config("spark.sql.session.timeZone", "+11")
            .getOrCreate()
        )

    def __del__(self):
        self.sp.stop
        print("Processing BNPL data completed!")

    def transform(self):
        """
        Main function to call all the processing steps and methods
        """
        # Read data from the processed folder

        # List all the functions to process in order

        # Output code to specified folder


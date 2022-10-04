import os
import utils as u

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, round

class Clean():
    """
    Class dedicated for cleaning internal and external datasets
    """

    def __init__(self):
        self.sp = (
            SparkSession.builder.appName("Clean BNPL")
            .config("spark.sql.session.timeZone", "+11")
            .getOrCreate()
        )
        self.transactions = u.read_tables(self.sp, "transactions")

    def __del__(self):
        self.sp.stop
        print("Clean Up Completed!")

    def clean_all(self):
        """
        Function to clean all code and output into the curated data folder
        """
        
        # Call list of cleaning functions
        self.dollar_value()
        self.drop_cols()

        # Write final data into curated folder
        u.write_data(self.transactions, "curated", "transactions")

    def clean_tax_income(self):
        # Update to include tax income data (EXAMPLE ONLY)
        tax_income = u.read_tables(self.sp, "tax_income", "c")
        pass

    def dollar_value(self):
        """
        Function to clean dollar values and remove the noise associated with the data
        """
        # round each transaction to the nearest cent
        self.transactions = self.transactions.withColumn("dollar_value", round(col("dollar_value"), 2))
        self.transactions = self.transactions.withColumn("dollar_value", col("dollar_value").cast(FloatType()))

    def drop_cols(self):
        """
        Function to drop unnecessary columns
        """
        self.transactions = self.transactions.drop("order_id")

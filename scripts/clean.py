import os
import utils as u

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

    def __del__(self):
        self.sp.stop
        print("Clean Up Completed!")

    def clean_tax_income(self):
        # Update to include tax income data (EXAMPLE ONLY)
        tax_income = u.read_tables(self.sp, "tax_income", "c")
        pass

    def dollar_value(self):
        """
        Function to clean dollar values and remove the noise associated with the data
        """
        
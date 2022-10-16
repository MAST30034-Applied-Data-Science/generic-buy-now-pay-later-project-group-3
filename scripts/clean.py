import utils as u

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, round, monotonically_increasing_id

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
        self.c_fraud = u.read_tables(self.sp, "consumer_fraud_probability" , "c")
        self.m_fraud = u.read_tables(self.sp, "merchant_fraud_probability", "c")
        

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
        self.convert_data()
        self.change_column()

        # Write final data into curated folder
        self.write_all()
        

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
        # Replace order_id with index
        self.transactions = self.transactions.drop("order_id")
        self.transactions = self.transactions.withColumn("order_id", monotonically_increasing_id())

    def convert_data(self):
        """
        Function to convert datatypes of the following probabilites
            - Convert fraud probabilites column from string to float
        """
        self.c_fraud = self.c_fraud.withColumn("fraud_probability", col("fraud_probability").cast(FloatType()))
        self.m_fraud = self.m_fraud.withColumn("fraud_probability", col("fraud_probability").cast(FloatType()))

    def change_column(self):
        """
        Function to change consumer_id to user_id in the consumer table
        """
        consumer = u.read_tables(self.sp, "tbl_consumer")
        lookup = u.read_tables(self.sp, "consumer_user_details", "p").drop("name", "address")
        u.write_data(consumer.join(lookup, on="consumer_id").drop("consumer_id"), "curated", "consumer_details")

    def write_all(self):
        """
        Function to write all cleaned data into curated
        """
        folder = "curated"

        # Files to write
        u.write_data(self.transactions, folder, "transactions")
        u.write_data(self.c_fraud, folder, "customer_fraud")
        u.write_data(self.m_fraud, folder, "merchant_fraud")

        print("Files have been written")


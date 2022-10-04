import os
import pandas as pd
from functools import reduce

import utils as u

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, countDistinct, date_format

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
        
        # Variables
        self.transactions = u.read_curated(self.sp, "transactions")
        self.merchants = u.read_tables(self.sp, "tbl_merchants", "p")
        self.customers = u.read_tables(self.sp, "tbl_consumer", "c")

    def __del__(self):
        self.sp.stop
        print("Processing BNPL data completed!")

    def transform(self):
        """
        Main function to call all the processing steps and methods
        """
        # List all the functions to process in order
        self.merchant_transform()

        # Output code to specified folder
        u.write_data(self.merchants, "processed", "merchants")

    def merchant_transform(self):
        """
        Call all functions with regards to merchants
        """
        print("In merchant transformation function")
        self.merchants.show(3)

        # Get unregistered column counts
        unregistered = self.unregistered_customers(self.merchants, self.customers, self.transactions)
        self.merchants = self.create_columns(unregistered, self.merchants)
        self.merchants = self.create_cust_growth_column(self.merchants, self.transactions)

        print("After Mechant processing")
        self.merchants.show(3)

    def unregistered_customers(self, merchants, customers, transactions):
        '''
        Args:
            merchants (pyspark.sql.DataFrame)    : Df with details about all the  merchants, including their 'merchant_abn'

            customers (pyspark.sql.DataFrame)    : Df with details about all the customers, including their 'consumer_id'

            transactions (pyspark.sql.DataFrame) : Df with details about all the transactions made between merchants and customers

        Returns:
            A pyspark.sql.DataFrame with all the transactions that have a registered Merchant ABN but an unknown user/customer ID.
        '''
        
        # list of registered merchant ABNs
        abn_list = merchants.rdd.map(lambda x: x.merchant_abn).collect()

        # transactions with registered merchant ABNs
        reg_merchant_trans = transactions[transactions.merchant_abn.isin(abn_list)]

        # total transactions with unidentified customers
        unknown_cust = (transactions.select('user_id').distinct()) \
                        .subtract(customers.select(col('consumer_id')))
        unknown_cust_list = unknown_cust.rdd.map(lambda x: x.user_id).collect()

        # transactions with registered merchant ABNs but unknown customer IDs
        return reg_merchant_trans[reg_merchant_trans.user_id.isin(unknown_cust_list)]

    def create_columns(self, unknown_cust_trans, merchants):
        '''
        Args:
            unknown_cust_trans (pyspark.sql.DataFrame) : Df with all the transactions that have a registered Merchant ABN but an unknown user/customer ID.
            
            merchants (pyspark.sql.DataFrame)          : Df with details about all the  merchants, including their 'merchant_abn'

        Returns:
            Updated 'merchants' df with two new columns.
        '''

        # number of transactions with unknown users for each merchant 
        trans_count = unknown_cust_trans.groupBy("merchant_abn").count() \
                        .withColumnRenamed("count", "unknown_user_trans_count")

        # number of unknown customers for each merchant
        users_count = unknown_cust_trans.groupBy("merchant_abn") \
                        .agg(countDistinct("user_id")) \
                        .withColumnRenamed("count(user_id)", "unknown_unique_users_count")

        # add relevant counts as new columns to the merchant dataset
        merchants = merchants.join(trans_count, ["merchant_abn"])
        merchants = merchants.join(users_count, ["merchant_abn"])

        return merchants

    def create_cust_growth_column(self, merchants, transactions):
        '''
        Args:
            merchants (pyspark.sql.DataFrame)    : Df with details about all the  merchants, including their 'merchant_abn'

            transactions (pyspark.sql.DataFrame) : Df with details about all the transactions made between merchants and customers

        Returns:
            Updated 'merchants' df with one new column.
        '''

        # add monthly customer increase as a new column to the merchant dataset
        cust_growth = self.aggregate_monthly(transactions)
        merchants = merchants.join(cust_growth, ["merchant_abn"])

        return merchants

    def aggregate_monthly(self, transactions):
        '''
        Args:
            transactions (pyspark.sql.DataFrame) : Df with details about all the transactions made between merchants and customers

        Returns:
            A pyspark.sql.DataFrame with the average monthly increase in the number of customer for every merchant_abn
        '''
        monthly_trans = transactions.withColumn("order_month", 
                                    date_format('order_datetime','yyyy-MM'))
        monthly = monthly_trans.groupBy("merchant_abn", "order_month").agg(countDistinct('user_id')).withColumnRenamed("count(user_id)", "distinct_customers")
        sorted_monthly = monthly.sort(['merchant_abn', 'order_month'])

        return self.get_monthly_increase(sorted_monthly.toPandas())
        
    def get_monthly_increase(self, monthly_df):
        '''
        Args:
            monthly_df (pandas.DataFrame) : Df with the distinct number of customers that made transactions with a particular merchant every month

        Returns:
            A pyspark.sql.DataFrame with the average monthly increase in the number of customer for every merchant_abn
        '''
        curr_abn = monthly_df['merchant_abn'][0]
        differences = []
        abns = []
        incs = []
        for i in range(monthly_df.shape[0] - 1):
            if monthly_df['merchant_abn'][i] != curr_abn:
                abns.append(curr_abn)
                incs.append(sum(differences) / len(differences))

                curr_abn = monthly_df['merchant_abn'][i]
                differences = []

            differences.append(monthly_df['distinct_customers'][i+1] - monthly_df['distinct_customers'][i])

        growth = pd.DataFrame.from_dict({"merchant_abn": abns, "avg_monthly_inc": incs})
        return self.sp.createDataFrame(growth)


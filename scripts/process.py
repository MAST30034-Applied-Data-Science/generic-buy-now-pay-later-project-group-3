import os
import pandas as pd

import utils as u

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, countDistinct, date_format, expr, count, when

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
        self.customers = u.read_curated(self.sp, "consumer_details")

    def __del__(self):
        self.sp.stop
        print("Processing BNPL data completed!")

    def transform(self):
        """
        Main function to call all the processing steps and methods
        """
        # MERCHANTS
        self.merchant_transform()
        u.write_data(self.merchants, "processed", "merchants")

        # TRANSACTIONS
        self.transaction_transform()
        u.write_data(self.transactions, "processed", "transactions")

    def merchant_transform(self):
        """
        Call all functions with regards to merchants
        """
        # Growth features
        self.merchants = self.create_cust_growth_column(self.merchants, self.transactions)

    def transaction_transform(self):
        """
        Call all functions with regard to transactions
        """
        self.transactions = self.potential_outlier(self.transactions)
        self.transactions = self.get_holidays()

    def get_holidays(self):
        """
        Merge holiday data with transactions
        """
        holiday = self.sp.read.option("inferSchema", True).parquet("../data/tables/holiday")
        return self.transactions.join(holiday, holiday.date == self.transactions.order_datetime, how="left").drop("date")

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

    def potential_outlier(self, full_dataset):
        '''
        # Outlier detection algorithm

        This will be inplemented by creating an attribute called 'potential outlier'. which marks dollar 
        values of transactios that fall out of a companies specific SIQR as True, and False otherwise. 
        Furthermore, it marks all transactions that belong to a company, which has no variance 
        in the dollar value of respective transactios. This is due to it being unrealistic/dodgy.
        
        Note: after further investigating the 'dodgy' transactions, consistent dollar values for all merchant's 
        should be reconsidered, as some fall under the 'tv subscription' description, which should be consistent
        anyway
        '''
        Fst_percentile = expr('percentile_approx(dollar_value, 0.25)')
        Trd_percentile = expr('percentile_approx(dollar_value, 0.75)')
        Second_percentile = expr('percentile_approx(dollar_value, 0.5)')

        Outlier_tags = full_dataset.groupBy('merchant_abn').agg(Fst_percentile.alias('1_val'), Trd_percentile.alias('3_val'), Second_percentile.alias('2_val'), count('dollar_value').alias('Count'))
        Outlier_tags = Outlier_tags.withColumn('SIQR_Lower', col('2_val') - col('1_val'))
        Outlier_tags = Outlier_tags.withColumn('SIQR_Upper', col('3_val') - col('2_val'))

        # Now calculate the limits
        Outlier_tags = Outlier_tags.withColumn('Upper_limit', col('3_val') + 3 * col('SIQR_Upper'))
        Outlier_tags = Outlier_tags.withColumn('Lower_limit', col('1_val') - 3 * col('SIQR_Lower'))

        # after noticing that some merchants only have one transaction value (i.e one dollar_value amount for all transactios)
        # decided to removed due to unrealisic distributed data 
        Outlier_tags = Outlier_tags.withColumn('Natural_var', when((col('Upper_limit') == col('Lower_limit')) & (col('Count') > 10), 1).otherwise(0))
        Outlier_tags = Outlier_tags.select('merchant_abn', 'Upper_limit', 'Lower_limit', 'Natural_var')
        
        # Now all we need to do is join this data to each transaction, then can select the transactios which are (not) within the limits
        Outlier_tags = full_dataset.select('merchant_abn', 'order_id', 'user_id', 'dollar_value').join(Outlier_tags, on= ['merchant_abn'])
        
        # finally identify the outliers which fall out of distribution or apart of a dodgy business
        Outlier_tags = Outlier_tags.withColumn('Potential_Outlier', when((Outlier_tags.dollar_value <= col('Upper_limit')) & (Outlier_tags.dollar_value >= col('Lower_limit')) & (col('Natural_var') == 0), 0)
                                                    .otherwise(1))
        # Join the new attributes obtained above to the transaction spark dataframe
        Outlier_tags = Outlier_tags.select(['order_id', 'Natural_var', 'Potential_Outlier'])
        full_dataset = full_dataset.join(Outlier_tags, on='order_id')
        return full_dataset
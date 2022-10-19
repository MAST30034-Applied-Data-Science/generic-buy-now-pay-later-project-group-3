

class Ranking():
    """
    Class dedicated to implementing the ranking algorithm for merchants.
    """
    def __init__(self, dir = None):
        self.sp = (
            SparkSession.builder.appName("Ranking system")
            .config("spark.sql.session.timeZone", "+11")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config('spark.sql.parquet.cacheMetadata', 'True')
            .getOrCreate()
        )
        self.dir = dir

    def __del__(self):
        self.sp.stop
        print("Merchant scoring completed!")

    def ranking_system(self):
        """
        Driver function to run the ranking algorithm
        """
        customers = u.read_processed(self.sp, 'customers')
        transactions = u.read_processed(self.sp, 'transactions')
        merchants = u.read_processed(self.sp, 'merchants')

        desc_lookup = self.sp.read.option("header", False).csv(self.dir + "description_lookup.csv")
        fraud_probabilities = self.sp.read.option("inferSchema", True).parquet(self.dir + '../../models/random_forest_output_full/')

        final_data_collection = merchants.select('merchant_abn')
        fraud_probabilities = fraud_probabilities.select('merchant_abn', 'user_id', 'order_datetime', 'prediction')

        merchants = merchants.join(desc_lookup, (merchants.tags==desc_lookup._c1)).drop('tags')
        merchants = merchants.withColumnRenamed('_c0', 'Description').drop('_c1')

        # calculate the BNPL unweighted revenue 
        full_dataset = transactions.join(merchants, on='merchant_abn', how='inner').withColumn('BNPL_Revenue', F.col('dollar_value') * F.col('BNPL_Fee'))

        # Data range = last 6 months (march onwards) 
        RECENCY =  F.lit('2022-03-01')

        # run functions to calculate the four scores for each merchant
        self.financial_score(RECENCY, full_dataset, fraud_probabilities)
        # sustainability_score(merchants, full_dataset)
        # customer_score(full_dataset, customers)
        # industry(full_dataset, fraud_probabilities, spark, RECENCY)

        print("successful")

    def feature_standardisation(self, dataset, max_columns, min_columns = []): 
        """
        Function for scaling every individual score
        """
        # if higher values are prefered
        for col_name in max_columns:
            values = dataset.select(F.max(col_name).alias('high'), F.min(col_name).alias('low'))
            dataset = dataset.withColumn(col_name, F.round((F.col(col_name) - values.select('low').head()[0]) / (values.select('high').head()[0] - values.select('low').head()[0]), 4))

        # if lower values are prefered
        if min_columns:
            for col_name in min_columns:
                values = dataset.select(F.max(col_name).alias('high'), F.min(col_name).alias('low'))
                dataset = dataset.withColumn(col_name, F.round((values.select('high').head()[0] - F.col(col_name)) / (values.select('high').head()[0] - values.select('low').head()[0]), 4))
        return dataset

    def revenue_growth_score(self, revenue_data):
        """
        Function to calculate the revenue growth score
        """
        revenue_data_table = revenue_data.select('merchant_abn').distinct()
        
        months = [3,4,5,6,7,8,9]
        for month in months:
            end_month = month + 1
            if end_month < 10:
                revenue_data_table = revenue_data_table.join(revenue_data.where((F.col('order_datetime') < F.lit('2022-0' + str(end_month)+'-01'))).groupBy('merchant_abn').agg(F.sum('BNPL_weighted_Revenue').alias('Month_' + str(month))), on='merchant_abn', how='left')
            else:
                revenue_data_table = revenue_data_table.join(revenue_data.where((F.col('order_datetime') < F.lit('2022-' + str(end_month)+'-01'))).groupBy('merchant_abn').agg(F.sum('BNPL_weighted_Revenue').alias('Month_' + str(month))), on='merchant_abn', how='left')
        revenue_data_table = revenue_data_table.fillna(1)

        # Now need to calculate growth rate for each month
        revenue_data_table = revenue_data_table.withColumn('Growth_4', (F.col('Month_4') - F.col('Month_3')) / F.col('Month_4'))
        revenue_data_table = revenue_data_table.withColumn('Growth_5', (F.col('Month_5') - F.col('Month_4')) / F.col('Month_5'))
        revenue_data_table = revenue_data_table.withColumn('Growth_6', (F.col('Month_6') - F.col('Month_5')) / F.col('Month_6'))
        revenue_data_table = revenue_data_table.withColumn('Growth_7', (F.col('Month_7') - F.col('Month_6'))/ F.col('Month_7'))
        revenue_data_table = revenue_data_table.withColumn('Growth_8', (F.col('Month_8') - F.col('Month_7')) / F.col('Month_8'))
        revenue_data_table = revenue_data_table.withColumn('Growth_9', (F.col('Month_9') - F.col('Month_8')) / F.col('Month_9'))

        revenue_data_table = revenue_data_table.withColumn('Revenue_Growth_Avg', F.round((F.col('Growth_4') + F.col('Growth_5') + F.col('Growth_6') + F.col('Growth_7') + F.col('Growth_8') + F.col('Growth_9')) / 6 , 4))
        revenue_data_table = revenue_data_table.select('merchant_abn', 'Revenue_Growth_Avg')
        return revenue_data_table


    def financial_score(self, RECENCY, transactions, fraud_data):
        """
        Function to score each merchant on their financial aspects
        """
        revenue_data = transactions.where(F.col('order_datetime') > RECENCY).select('merchant_abn','user_id', 'order_datetime', 'dollar_value', 'BNPL_Revenue')
        # Now join fraud prob 
        revenue_data = revenue_data.join(fraud_data, on=['merchant_abn', 'user_id', 'order_datetime'], how='left')
        # Firstly, calculate the revenue score

        # Weight the revenue weighted with probability
        revenue_data = revenue_data.withColumn('BNPL_weighted_Revenue', F.col('BNPL_Revenue') * (1 - 0.1 * F.col('prediction')))
        # Now can caluculate each Merchants revenue
        revenue_final = revenue_data.groupBy('merchant_abn').agg(F.round(F.sum('BNPL_weighted_Revenue'), 2).alias('Total_Revenue'))
        # Next, get the revenue growth score 
        revenue_final = revenue_final.join(self.revenue_growth_score(revenue_data), on='merchant_abn', how='left')
        # Finally scale
        revenue_final = self.feature_standardisation(revenue_final, ['Revenue_Growth_Avg', 'Total_Revenue'])
        # save file 
        revenue_final.write.parquet('../' + dir + 'curated/Metric_Finantial_scaled', mode ='overwrite')
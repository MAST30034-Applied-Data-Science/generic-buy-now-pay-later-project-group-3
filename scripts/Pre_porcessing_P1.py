import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string
import re
import pandas as pd
from pyspark.sql import SparkSession, functions as F
import os
import glob
import numpy as np
from pyspark.sql.types import *
#nltk.download('stopwords')
#nltk.download('wordnet')
#nltk.download('omw-1.4')

# Functions
#########################################################################################
# This is used to standardise the description of each merchant
def text_process(text):
    stemmer = WordNetLemmatizer()
    # Remove all punctuation and numbers 
    nopunc = [char for char in text if char not in string.punctuation]
    nopunc = ''.join([i for i in nopunc if not i.isdigit()])
    # Remove all stopwords
    nopunc =  [word.lower() for word in nopunc.split() if word not in stopwords.words('english')]
    # lemmatize and output
    return ' '.join([stemmer.lemmatize(word) for word in nopunc])

def tax_add(dataset):
    tax_data = pd.read_csv("../data/tables/tax_income.csv")
    tax_data['Postcode'] = tax_data['Postcode'].astype('str')
    # First remove duplicate columns 
    tax_data = tax_data.T.drop_duplicates().T
    tax_columns = tax_data.columns[1:]
    # IMPUTATION TIME
    postcodes_agg = dataset.join(tax_data, lsuffix='postcode', rsuffix='Postcode', how='left')
    for col in tax_columns:
        for agg_name in ['sa4name', 'electorate', 'electoraterating']:
            postcodes_agg[col] = postcodes_agg.groupby(agg_name)[col].transform(lambda x: x.fillna(x.mean()))
        postcodes_agg[col] = postcodes_agg[col].apply(np.ceil).astype('int')
    return postcodes_agg, tax_columns

# This function joins an external dataset to the cunstomer details, in particular adding regions/electorates... 
# Based on postcode
def postcode_add(dataset, spark):
    postcodes = pd.read_csv("../data/tables/australian_postcodes.csv")
    cust = dataset.toPandas()
    postcodes = postcodes[['postcode', 'state', 'sa3name', 'sa4name', 'SA3_NAME_2016', 'electoraterating', 'electorate']]
    # First imputate missing values
    for col in postcodes.columns[1:]:
        postcodes[col] = postcodes.groupby("state")[col].transform(lambda x: x.fillna(x.mode()))
    postcodes_agg = postcodes.groupby(['state', 'postcode'], as_index=False).agg(sa3name = pd.NamedAgg('sa3name',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 sa4name = pd.NamedAgg('sa4name',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 electoraterating = pd.NamedAgg('electoraterating',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 SA3_NAME_2016 = pd.NamedAgg('SA3_NAME_2016',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 electorate = pd.NamedAgg('electorate',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN)
                                                 )
                                                 # Imputate
    imputation = postcodes_agg.groupby('state', as_index=False).agg(sa3name_mode = pd.NamedAgg('sa3name',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 sa4name_mode = pd.NamedAgg('sa4name',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 electoraterating_mode = pd.NamedAgg('electoraterating',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 SA3_NAME_2016_mode = pd.NamedAgg('SA3_NAME_2016',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN),
                                                 electorate_mode = pd.NamedAgg('electorate',lambda x: pd.Series.mode(x) if len(pd.Series.mode(x))>0 else np.NaN)
                                                 )
    postcodes_agg = postcodes_agg.merge(imputation, on='state', how='left')
    postcodes_agg.sa3name.fillna(postcodes_agg.sa3name_mode, inplace=True)
    postcodes_agg.sa4name.fillna(postcodes_agg.sa4name_mode, inplace=True)
    postcodes_agg.electoraterating.fillna(postcodes_agg.electoraterating_mode, inplace=True)
    postcodes_agg.SA3_NAME_2016.fillna(postcodes_agg.SA3_NAME_2016_mode, inplace=True)
    postcodes_agg.electorate.fillna(postcodes_agg.electorate_mode, inplace=True)
    postcodes_agg = postcodes_agg.drop(['sa3name_mode', 'sa4name_mode', 'electoraterating_mode', 'SA3_NAME_2016_mode', 'electorate_mode'], axis = 1)
    # now add the tax information to each 
    postcodes_agg, tax_columns = tax_add(postcodes_agg) 
    postcodes_agg.set_index(['state', 'postcode'], inplace = True)
    cust.set_index(['state', 'postcode'], inplace= True)
    customer_tbl = cust.join(postcodes_agg, how='left')
    customer_tbl = customer_tbl.reset_index()
    customer_tbl.drop(columns='Postcode', inplace = True)
    # create schema
    structure = []
    for att_name in customer_tbl.columns:
            if att_name not in tax_columns:
                    structure.append(StructField(att_name, StringType(), True))
            else: 
                    structure.append(StructField(att_name, IntegerType(), True))
    schema = StructType(structure)
    # convert back to original form
    customer_tbl = spark.createDataFrame(customer_tbl, schema)
    return customer_tbl

# this function standardises the tags attribute, creating a list with the 'description', 'revenue band' and 'BNPL service charge'
def tag_extract(tag_string): 
    # first need to preprocess
    string =  re.sub('\[','(', tag_string.lower())
    string = re.sub('\]',')', string)
    # break the string into sections
    string_cut = string.split('),')
    new_string = []
    # first extract the description and pre process
    descr = str(string_cut[0].strip('(('))
    new_string.append(text_process(descr))
    # second extract the band
    new_string.append(str(re.search(r'[a-z]',string_cut[1]).group()))
    # finally the take rate
    new_string.append(float(re.search(r'[0-9]+\.[0-9]+',string_cut[2]).group()))
    return(new_string)

# This function takes the pandas dataframe containing merchant information and pre_processes it
def merchant_process(merchants, spark):
    merchants = merchants.toPandas()
    # Lets process the tags
    tags = merchants['tags']
    processed_tags = []
    for i in tags:
        processed_tags.append(tag_extract(i))
    merchant_tbl = pd.DataFrame(processed_tags, columns=('Description', 'Earnings_Class', 'BNPL_Fee'))
    merchant_tbl = pd.concat([merchants, merchant_tbl], axis=1)
    # drop the tags column 
    merchant_tbl.drop(columns='tags', inplace=True)
    merchant_tbl = spark.createDataFrame(merchant_tbl)
    return merchant_tbl

# This function reads all the transaction files in the curated section of the directory
def read_all_transactions(input_dir, spark):
    files = glob.glob(os.path.join(input_dir, 'transactions_*'))
    transaction = spark.read.parquet(files.pop(0))
    for file in files:
        transaction_add = spark.read.parquet(file)
        transaction.unionByName(transaction_add, True)
    return transaction

# This function is used to refine the final dataset
def full_dataset_refine(full_dataset):
    full_dataset = full_dataset.withColumn('Day', F.dayofweek('order_datetime'))
    full_dataset = full_dataset.withColumn('Month', F.month('order_datetime'))
    # now we can round each dollar value to the nearest cent (not 5 cents, as there exists unusual pricing in the real world)
    full_dataset = full_dataset.withColumn("dollar_value", F.round(F.col("dollar_value"), 2))
    # now we can also add the bnpl revenue from a transaction 
    full_dataset = full_dataset.withColumn('BNPL_Revenue', F.col('dollar_value') * 0.01 * F.col('BNPL_Fee'))
    return full_dataset

# This opens the spark session 
def open_spark():
    spark = (
    SparkSession.builder.appName("Data_Explorer")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate())
    return spark

# Description for the function is below
def potential_outlier(full_dataset):
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
    # In a similar method to the boxplot, we use the SIQR to identify outliers
    # see https://towardsdatascience.com/outlier-detection-part-2-6839f6199768
    Fst_percentile = F.expr('percentile_approx(dollar_value, 0.25)')
    Trd_percentile = F.expr('percentile_approx(dollar_value, 0.75)')
    Second_percentile = F.expr('percentile_approx(dollar_value, 0.5)')
    Outlier_tags = full_dataset.groupBy('merchant_abn').agg(Fst_percentile.alias('1_val'), Trd_percentile.alias('3_val'), Second_percentile.alias('2_val'), F.count('dollar_value').alias('Count'))
    Outlier_tags = Outlier_tags.withColumn('SIQR_Lower', F.col('2_val') - F.col('1_val'))
    Outlier_tags = Outlier_tags.withColumn('SIQR_Upper', F.col('3_val') - F.col('2_val'))
    # Now calculate the limits
    Outlier_tags = Outlier_tags.withColumn('Upper_limit', F.col('3_val') + 3 * F.col('SIQR_Upper'))
    Outlier_tags = Outlier_tags.withColumn('Lower_limit', F.col('1_val') - 3 * F.col('SIQR_Lower'))
    # after noticing that some merchants only have one transaction value (i.e one dollar_value amount for all transactios)
    # decided to removed due to unrealisic distributed data 
    Outlier_tags = Outlier_tags.withColumn('Natural_var', F.when((F.col('Upper_limit') == F.col('Lower_limit')) & (F.col('Count') > 10), True).otherwise(False))
    Outlier_tags = Outlier_tags.select('merchant_abn', 'Upper_limit', 'Lower_limit', 'Natural_var')
    # Now all we need to do is join this data to each transaction, then can select the transactios which are (not) within the limits
    Outlier_tags = full_dataset.select('merchant_abn', 'order_id', 'user_id', 'dollar_value').join(Outlier_tags, on= ['merchant_abn'])
    # finally identify the outliers which fall out of distribution or apart of a dodgy business
    Outlier_tags = Outlier_tags.withColumn('Potential_Outlier', F.when((Outlier_tags.dollar_value <= F.col('Upper_limit')) & (Outlier_tags.dollar_value >= F.col('Lower_limit')) & (F.col('Natural_var') == False), False)
                                                .otherwise(True))
    # Join the new attributes obtained above to the transaction spark dataframe
    Outlier_tags = Outlier_tags.select(['order_id', 'Natural_var', 'Potential_Outlier'])
    full_dataset = full_dataset.join(Outlier_tags, on='order_id')
    return full_dataset

def main():
    # open spark
    spark = open_spark()
    data_dir = '../data/tables/'
    transactions = read_all_transactions(data_dir, spark)
    consumer_details = spark.read.parquet(os.path.join(data_dir, 'consumer_user_details.parquet'))
    merchants_tbl = spark.read.parquet(os.path.join(data_dir,'tbl_merchants.parquet'))
    customer_tbl = spark.read.option("delimiter", "|").option("header",True).csv(os.path.join(data_dir,'tbl_consumer.csv'))
    # Process the merchants
    merchants = merchant_process(merchants_tbl, spark)
    # Join the datasets
    customer_tbl = customer_tbl.join(consumer_details, ['consumer_id'])
    full_dataset = transactions.join(customer_tbl, ['user_id'])
    merchants = merchants.withColumnRenamed('name','company_name')
    full_dataset = full_dataset.join(merchants, ['merchant_abn'])
    # Now lets rename and standardise some of our attributes
    full_dataset = full_dataset_refine(full_dataset)
    # Finally, lets only keep the desirable features, then save the dataset
    # we can remove name, location and customerID for now, due to being unnnesesary attributes (although company_name could also be removed)
    # Now lets add the Potential outlier attribute to each transaction
    full_dataset = potential_outlier(full_dataset)
    full_dataset.write.parquet('../data/curated/full_dataset', mode='overwrite')

####################################################################################

# Code

# this just sets the current directory
os.chdir("/mnt/d/University/Applied Datascience/generic-buy-now-pay-later-project-group-3/scripts/")
# now just run the main function 
main()
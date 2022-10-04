"""functions for computing entropy for merchants.
    Once decided the functions could be directly used in process.py 
"""
from pyspark.sql.functions import date_format, countDistinct

import utils as u
from process import Process
from clean import Clean
cleaner = Clean()
cleaner.clean_all()
self = Process()
def compute_month_entropy(self):
    monthly_trans = self.transactions.withColumn("order_month", 
    date_format('order_datetime','yyyy-MM'))
    monthly = monthly_trans.groupBy("merchant_abn", "order_month").count()
    sorted_monthly = monthly.sort(['merchant_abn', 'order_month'])
    sorted_monthly.show(5)


#compute_month_entropy(self)

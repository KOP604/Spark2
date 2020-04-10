# -*- coding: utf-8 -*-
"""
Created on Sun Apr  5 21:02:47 2020

@author: Administrator
"""
import findspark
findspark.init()

# In[4]:
import pyspark
import random
# In[2]:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()


# In[ ]:

# import Python modules
import pandas as pd
import numpy as np
import datetime as dt

# In[ ]:
fraud_df = spark.read.csv('ccFraud.csv', inferSchema=True, header=True)
fraud_df.schema
fraud_df.show(3)
fraud_df.count() # 10M
fraud_df.columns
"""
['custID',
 'gender',
 'state',
 'cardholder',
 'balance',
 'numTrans',
 'numIntlTrans',
 'creditLine',
 'fraudRisk']
"""
vlist=fraud_df.columns
fraud_df.select('custId').count()
fraud_df.select('custId').distinct().count() #check dup
fraud_df=fraud_df.dropDuplicates()

import pyspark.sql.functions as F
fraud_df.agg(F.count('custId').alias('count'), \
           F.countDistinct('custId').alias('distinct')).show()

#print # missing by COL
fraud_df.agg(*[(1 - (F.count(c) / F.count('*'))).alias(c) \
             for c in vlist]).show()

fraud_df.groupby('gender').count().show()

numv=['balance','numTrans','numIntlTrans']
fraud_df.describe(numv).show()
fraud_df.

fraud_df.agg({'balance': 'skewness'}).show()
fraud_df.agg({'balance': 'mean', 'numTrans':'std'}).show()

"""
avg(), count(), countDistinct(), first(), kurtosis(),
max(), mean(), min(), skewness(), stddev(), stddev_pop(),
stddev_samp(), sum(), sumDistinct(), var_pop(), var_samp() and
variance()
"""


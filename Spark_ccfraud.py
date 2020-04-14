# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 20:35:59 2020
GIT comment: commit -m "Spark_ccfraud 2020-X-XX"
@author: Administrator PL
"""

import findspark
findspark.init()

import pyspark
import random
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Python modules
import pandas as pd
import numpy as np
import datetime as dt

""" 
raw data then partitioned by state 
fp='D:/Python/Data/ccfraud.csv'
fraud=spark.read.csv(fp, inferSchema=True, header=True)

#check partition
print(fraud.rdd.getNumPartitions())

#repartition by colum
fraud = fraud.repartition("state")
print(fraud.rdd.getNumPartitions()) #200
fraud.write.mode("overwrite").csv("D:/Python/Data/ccfraud_part.csv", header=True)
"""

#red back partitioned data
fpp="D:/Python/Data/ccfraud_part.csv"
fraud=spark.read.csv(fpp, inferSchema=True, header=True)
#fraud=spark.read.csv(fpp, inferSchema=True, header=True).toDF() Why toDF()??

print(fraud.rdd.getNumPartitions()) #5
fraud.createOrReplaceTempView('fraud')
fs=fraud.sample(False, 0.00001, seed=1999).toPandas()

fraud.select("state").distinct().count() #51
fraud.count()
fraud.printSchema()

fraud.describe("creditLine").show()
fraud.select("fraudRisk").distinct().count()
spark.sql("SELECT distinct fraudRisk, count(*) as FREQ FROM fraud GROUP BY 1").show()
spark.sql("SELECT distinct gender, count(*) as FREQ FROM fraud GROUP BY 1").show()
fraud.stat.crosstab("gender","fraudRisk").sort('gender_fraudRisk').show()
spark.sql("SELECT fraudRisk, mean(balance) as bal, mean(numTrans) as txn, \
          mean(creditLine) as cl FROM fraud GROUP BY 1").sort('fraudRisk').show()
#groupBy w filter
fraud.groupBy('state').avg('balance').filter('avg(balance)>4115').show()
fraud.groupBy('state').min('balance').filter('state>35').show()

import pyspark.sql.functions as F
fraud.groupBy('state').agg(F.max("balance"),F.mean("fraudRisk")).show()

fraud.filter('state<35').stat.crosstab('gender','fraudRisk').orderBy('gender_fraudRisk').show()
#I don't see the following piping is necessary
fraud.filter('state<35').filter('gender=2').stat.crosstab('gender','fraudRisk').orderBy('gender_fraudRisk').show()
fraud.filter('state<35 and gender=2').stat.crosstab('gender','fraudRisk').orderBy('gender_fraudRisk').show()

#cast data type
fraud = fraud.select('*', fraud["gender"].cast("string"))
fraud.dtypes







# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 20:35:59 2020

@author: Administrator
"""

fp='D:/Python/Data/ccfraud.csv'
fraud=spark.read.csv(fp, inferSchema=True, header=True)
print(fraud.rdd.getNumPartitions())

#repartition
fraud = fraud.repartition(10)
print(fraud.rdd.getNumPartitions())

fraud.createOrReplaceTempView('fraud')

fraud.count()
fraud.printSchema()
fs=fraud.sample(False, 0.00001, seed=1999).toPandas()
fraud.describe("creditLine").show()
fraud.select("fraudRisk").distinct().count()
spark.sql("SELECT distinct fraudRisk, count(*) as FREQ FROM fraud GROUP BY 1").show()
spark.sql("SELECT distinct gender, count(*) as FREQ FROM fraud GROUP BY 1").show()
fraud.stat.crosstab("gender","fraudRisk").sort('gender_fraudRisk').show()
spark.sql("SELECT fraudRisk, mean(balance) as bal, mean(numTrans) as txn, \
          mean(creditLine) as cl FROM fraud GROUP BY 1").sort('fraudRisk').show()




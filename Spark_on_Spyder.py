# coding: utf-8
# ### Spark on Google Colab
#https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/api/sql/index.html
#I just gitted this code file

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

#"""Google sample data"""
#from google.colab import files
#files.upload()
# In[ ]:

df = spark.createDataFrame([{"hello": "world"} for x in range(10000)])
df.show(3)


# In[ ]:

# Sample Data
file_loc = './sample_data/california_housing_train.csv'
df = spark.read.csv(file_loc, inferSchema=True, header=True)
print(type(df))


# In[ ]:
df.show(5)

# In[ ]:
df.schema
# In[ ]:
df.dtypes
# In[ ]:
df.count()
# In[ ]:
df.select("longitude","households","median_income").show(10)

# In[ ]:
df.filter(df['households'] <200).show(10)
# In[ ]:
df.groupby('longitude').count().show(10)
# In[ ]:
df.filter(df["population"].isin(227.0)).show(10)
# In[ ]:
df2=df.filter(df["population"].isin(227.0))
# In[ ]:
df2.count()
# In[ ]:
df.filter(df["median_income"].between(2, 24)).show(10)
# In[ ]:
df.select("longitude","latitude",df["total_rooms"]+100).show(10) # when applying transformation use df["col"]
# In[ ]:
df.select("longitude","latitude","total_rooms").show(10)
# In[ ]:
from pyspark.sql import functions as F
df.select("*",F.when(df["median_income"] > 2, 1).otherwise(0).alias("inc_cat")).show(10) 
# In[ ]:
df.describe().show()
# In[ ]:
stat=df.describe()
stat
# In[ ]:
stat.columns
# In[ ]:
stat.head(3)
# In[ ]:
stat[2]
# In[ ]:
#add new column
df = df.withColumn("New",df["median_income"]/df["median_house_value"])
# In[ ]:
df.columns
# In[ ]:
df=df.drop("New")
# In[ ]:
df.columns
# In[ ]:
df.groupby('longitude').mean("median_income").show(10)
# In[ ]:
df_sorted=df.sort("total_rooms", ascending=False) 
# In[ ]:
df_sorted.show(10)
# In[ ]:
df_ordered=df.orderBy("total_rooms",ascending=False)
# In[ ]:
df_ordered.show(10)
# In[ ]:
#from pyspark.sql import functions as F
#df.withColumn('Room_cat', F.when(df["median_income"] > 1, 1).when(df["median_income"]>5,2).otherwise(0)).show(10)
#OR
df.select("*", F.when(df["median_income"] > 1, 1).when(df["median_income"]>5,2).otherwise(0).alias("Room_cat")).show(10)

# In[ ]:
df=df.withColumn('Room_cat', F.when(df["median_income"] > 5, 3).when(df["median_income"]>3,2).otherwise(1))
df.groupby("Room_cat").count().orderBy("Room_cat").show()
# In[ ]:
#output
df.select("*").write.save("CA_Housing.json",format="json")


# ################### Movies data
# 

# In[ ]:

movies = spark.read.csv('D:/Python/Data/Oscars.csv', inferSchema=True, header=True)

movies.select('_unit_id').count()
movies.select('_unit_id').distinct().count() #check dup
movies=movies.dropDuplicates()

print(movies.select([c for c in movies.columns if c !='_unit_id']).distinct().count())

import pyspark.sql.functions as F
movies.agg(F.count('_unit_id').alias('count'), \
           F.countDistinct('_unit_id').alias('distinct')).show()

#print # missing by COL
movies.agg(*[(1 - (F.count(c) / F.count('*'))).alias(c) \
             for c in vlist[3:6]]).show()


    

# In[ ]:
vlist=movies.columns
movies.select(vlist[1:7]).show(10)
movies.select(vlist[7:13]).show(10)
movies.select(vlist[13:19]).show(10)
movies.select(vlist[19:23]).show(10)
movies.select(vlist[23:]).show(10)

# In[ ]:
print(vlist)

#from pyspark.sql.types import DateType
#movies = movies.withColumn("BDT", movies[vlist[7]].cast(DateType()))
"""
['_unit_id', '_golden', '_unit_state', '_trusted_judgments', 
 '_last_judgment_at', 'birthplace', 'birthplace:confidence', 
 'date_of_birth', 'date_of_birth:confidence', 'race_ethnicity', 
 'race_ethnicity:confidence', 'religion', 'religion:confidence', 
 'sexual_orientation', 'sexual_orientation:confidence', 'year_of_award',
 'year_of_award:confidence', 'award', 'biourl', 'birthplace_gold', 
 'date_of_birth_gold', 'movie', 'person', 'race_ethnicity_gold',
 'religion_gold', 'sexual_orientation_gold', 'year_of_award_gold']
"""

# In[ ]:
movies.select('year_of_award').show(10)
movies.describe('year_of_award').show()
movies.describe('_unit_id').show()

movies.select("_unit_id").distinct().count()    
movies.select("_unit_state").distinct().show(10)
movies.groupBy("_unit_state").count().show()
movies.select("race_ethnicity").distinct().show(10)
movies.groupBy("race_ethnicity").count().show()

#Age
movies.select('date_of_birth').show(10)
movies.select('date_of_birth').dtypes

from pyspark.sql import functions as F
movies=movies.withColumn("BDT",F.to_date(movies["date_of_birth"], "d-MMM-yyyy"))
movies=movies.withColumn("BDT2",F.to_date(movies["date_of_birth"], "d-MMM-yy"))
             
movies.select('date_of_birth', 'BDT','BDT2').show(10)
movies.where(movies['BDT'].isNull()).select('date_of_birth', 'BDT','BDT2').show(10)
#alternatively
movies.where("BDT is Null").select('date_of_birth', 'BDT','BDT2').show(10)


#iamhere 

movies=movies.withColumn('YR1', F.year(movies['BDT']))
movies=movies.withColumn('YR2', F.year(movies['BDT2'])-100)

movies.drop('BYR','YR1','YR2')
from pyspark.sql.functions import when
movies=movies.withColumn('BYR', when(movies['BDT'].isNotNull(),movies['YR1']) \
                               .otherwise(movies['YR2']))
movies.describe('BYR').show()
movies.where(movies['BDT'].isNull()).select('date_of_birth', 'BDT','BDT2','YR1','YR2').show(10)
movies.where(movies['BDT'].isNotNull()).select('date_of_birth', 'BDT','BDT2','YR1','YR2').show(10)

movies.where(movies['BDT'].isNull()).select('date_of_birth', 'BYR').show(10)
movies.where(movies['BDT'].isNotNull()).select('date_of_birth', 'BYR').show(10)

movies.drop('YR1','YR2')

movies=movies.withColumn('Age', movies['year_of_award']-movies['BYR'])
movies.describe('Age').show()
movies.select(F.min('Age').alias('min'), F.max('Age').alias('max')).show()

                       
#'birthplace'
movies.select('birthplace').distinct().count()
movies.select('birthplace').show(10)

split_col = pyspark.sql.functions.split(df['my_str_col'], '-')
movies = movies.withColumn('city', F.split(movies['birthplace'],',').getItem(0))
movies = movies.withColumn('ST', F.split(movies['birthplace'],',').getItem(1))

movies.select('city','state','birthplace').show(10)
movies.groupBy('ST').count().sort('count',ascending=False).show(10)

movies.select('birthplace').filter(movies['ST'].isNull()).show(10)
movies.select('birthplace','city').filter(movies['state'].isNull()).distinct().show()

movies=movies.withColumn('ST',F.when(movies['birthplace']=='New York City','NY').otherwise(movies['ST']))
movies=movies.select('*', F.upper('ST').alias('STE'))
movies.select('STE').show(10)

#save to CSV
movies.write.csv("D:/Python/Data/movies.csv", header="true", mode="overwrite")
movies = spark.read.csv('D:/Python/Data/movies.csv', inferSchema=True, header=True)

#sexual orientation
movies.select('sexual_orientation').distinct().show()
movies.groupBy('sexual_orientation').count().sort('count',ascending=False).show()

#religion
movies.select('religion').distinct().count()
movies.select('religion').distinct().show()
movies.groupBy('religion').count().show()
movies.groupBy('religion').count().sort('count',ascending=False).show()

movies.stat.crosstab("religion","sexual_orientation").show()

#Race
movies.select("race_ethnicity").distinct().show()
movies.groupBy("race_ethnicity").count().show()

movies.stat.crosstab("religion","race_ethnicity").show()
movies.groupBy("race_ethnicity").mean('Age').show()
movies.groupBy("race_ethnicity").mean('race_ethnicity:confidence','Age').show()



# In[ ]:
#change data type
#from pyspark.sql.types import DoubleType
#movies = movies.withColumn("vote_avg_num", movies["vote_average"].cast(DoubleType()))
# In[ ]:
for c in vlist:
  K=movies.filter(movies[c].isNull()).count()
  print(c, K)
# In[ ]:
#["title"]
#fields = [StructField("title", StringType(), True)]
#schema = StructType(fields)

"""**************** SPARK SQL starts ************************************"""
mv = spark.read.csv('D:/Python/Data/Oscars.csv', inferSchema=True, header=True)
mv.createOrReplaceTempView("oscar2")

spark.sql("SELECT distinct _unit_state, count(*) as FREQ FROM oscar2 WHERE year_of_award>1900 \
          GROUP BY _unit_state").show(10)

#df2=spark.sql("SELECT * FROM oscar2 WHERE year_of_award>1900")
#spark.sql("SELECT * FROM df2 WHERE year_of_award>1903").show(10)
spark.sql("SELECT date_of_birth, to_date(date_of_birth, 'd-MMM-yyyy') as BDT FROM oscar2").show(10)
spark.sql("SELECT count(*) FROM oscar2 where to_date(date_of_birth, 'd-MMM-yyyy') is Null").show()
spark.sql("SELECT date_of_birth FROM oscar2 where to_date(date_of_birth, 'd-MMM-yyyy') is Null").show(10)
spark.sql("SELECT date_of_birth FROM oscar2 where to_date(date_of_birth, 'd-MMM-yyyy') is not null").show(10)

spark.sql("SELECT date_of_birth, to_date(date_of_birth, 'd-MMM-yy') \
          FROM oscar2 where to_date(date_of_birth, 'd-MMM-yyyy') is Null").show(10)

BYR=spark.sql("SELECT _unit_id, case WHEN to_date(date_of_birth, 'd-MMM-yyyy') is Null THEN \
          year(to_date(date_of_birth, 'd-MMM-yy'))-100 \
          ELSE year(to_date(date_of_birth, 'd-MMM-yyyy')) END as BYR FROM oscar2")

BYR.createOrReplaceTempView("BYR")
spark.sql("SELECT min(BYR), max(BYR)) from BYR").show(10)
spark.sql("SELECT count(*) from BYR WHERE BYR is Null").show(10)

spark.sql("SELECT CASE WHEN BYR>1900 THEN '1900+' ELSE '<1900' END as PRD, \
          count(distinct _unit_id) as _FREQ_ FROM BYR GROUP BY PRD").show()

TT=spark.sql("SELECT A.*, B.BYR FROM oscar2 A LEFT JOIN BYR B ON A._unit_id=B._unit_id")
TT.columns
TT.createOrReplaceTempView("movies")

spark.sql("SELECT race_ethnicity as race, mean(year_of_award-BYR) as Age \
          ,min(year_of_award-BYR) as Min, max(year_of_award-BYR) as Max \
                    from movies GROUP BY race").show()

spark.sql("SELECT date_of_birth, to_date(date_of_birth, 'd-MMM-yy') as BDT \
          FROM oscar2 where date_of_birth is Null").show(10) #can't use calculated column?

spark.sql("SELECT religion FROM oscar2 LIMIT 10").show()

#sampling
spark.sql("SELECT _unit_id, birthplace FROM oscar2 TABLESAMPLE (5 PERCENT)").show(11)

#nest
spark.sql("SELECT a._unit_id, a.birthplace, b.religion FROM oscar2 a, oscar2 b \
          WHERE a._unit_id = b._unit_id AND b.religion='Buddhist' LIMIT 10").show()

# EXIT spark
#spark.stop() not necessary
    




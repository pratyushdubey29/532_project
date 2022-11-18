import pyspark
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession

import findspark
findspark.init()

spark = SparkSession.builder \
.master("local") \
.config("spark.driver.extraClassPath","C:/Users/AnshumaanChauhan/Documents/spark-3.3.0-bin-hadoop3/spark-3.3.0-bin-hadoop3/jars/mysql-connector-java-5.1.48.jar") \
.config("spark.driver.memory","15g") \
.appName("Scalability Check of Systems for ML applications") \
.getOrCreate()

from pyspark.sql.functions import col, count, isnan, when

dataset = spark.read.csv('C:\\Users\AnshumaanChauhan\\Documents\\Systems for DS Umass\\Project\\archive (5)\\DelayedFlights.csv',
                         header=True)


#Checking which column has null or nan values how many times 
dataset.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataset.columns]).show(vertical=True)

dataset.select(col("ArrDelay")).where((col("ArrDelay").isNull() | isnan(col("ArrDelay"))) & (col('Diverted')==1)).count()

#For all the flights that are diverted their ActualElaspedTime, ArrDelay and AirTime is null, because they did not land at correct destination
dataset.select(*(col(c) for c in dataset.columns)).where((col("TaxiIn").isNull() | isnan(col("TaxiIn")))).count()

dataset.select(*(col(c) for c in dataset.columns)).where((col("TaxiIn").isNull() | isnan(col("TaxiIn"))) & (col("Diverted")==0)).count()

dataset.select(*(col(c) for c in dataset.columns)).where((col("TaxiIn").isNull() | isnan(col("TaxiIn"))) & (col("Diverted")==0) & (col("CancellationCode")=='N')).count()

#As we can infer above the TaxiIn are ArrTime are Null a few times. But it is the case when the flight is either cancelled or diverted. So it is fine if we not exclude the null values

dataset.select(*(col(c) for c in dataset.columns)).where((col("TaxiOut").isNull() | isnan(col("TaxiOut")))).show(vertical=True)

dataset.select(*(col(c) for c in dataset.columns)).where(((col("TaxiOut").isNull() | isnan(col("TaxiOut")))) & (col("CancellationCode")=='N')).show(vertical=True)

#We can infer from the above queries that TaxiOut is null only in the case of a cancelled flight. So it is fine if these are converted to 0s after type casting

dataset.select(*(col(c) for c in dataset.columns)).where((col("CRSElapsedTime").isNull() | isnan(col("CRSElapsedTime")))).show(vertical=True)

dataset.select(*(col(c) for c in dataset.columns)).where((col("CRSElapsedTime").isNull() | isnan(col("CRSElapsedTime"))) & (col("Diverted")==0)).count()

#We can infer from the above queries that CRSElapsedTime is null only in the case of a diverted flight. So it is fine if these are converted to 0s after type casting
# Last columns have null values if the flight is cancelled instead of getting delayed, we do not drop these columns. Instead when we type cast the column to numeric data type these will be converted to zero which makes sense

dataset.count()

dataset.select("_c0").distinct().count()

#Means that _c0 column is just for indexing the entreis, therefore we will use this column for joining the 2 datasets
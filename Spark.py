import boto
from datetime import datetime
from time import time
import pandas as pd
from pyspark.sql.functions import split
from pyspark.sql.functions import udf

s = time()
raw = spark.read.json("s3a://strenuousfx/2017/04/*/*/*")
raw.cache()
print(time() - s)

USD_JPY = raw.filter(raw["instrument"] == "USD_JPY").selectExpr("time","instrument","closeoutBid", "closeoutAsk")
USD_JPY.cache()
USD_JPY.head(10)
USD_JPY.write.parquet("s3a://strenuousfx/output/USD_JPY")

USDJPY = spark.read.parquet("s3a://strenuousfx/output/USD_JPY")

USDJPY.createOrReplaceTempView("USDJPY")
USDJPY_limit_20 = spark.sql("SELECT * FROM USDJPY ORDER BY time ASC limit 20")
USDJPY_limit_20.show()

USDJPY_average = spark.sql("SELECT instrument, AVG(closeoutBid) , AVG(closeoutAsk) FROM USDJPY GROUP BY instrument")
USDJPY_average.show()

Days = spark.sql("SELECT DISTINCT(split(time,'T')[0]) as Day FROM USDJPY ORDER BY Day")
Days.show()
day = Days.count() - 1
week = day / 6
print(day, week)

USDJPY_hour = spark.sql("SELECT * FROM USDJPY ORDER BY time ASC")
USDJPY_hour = USDJPY_hour.withColumn('hour', split(split('time', 'T')[1], ':')[0])
USDJPY_hour.show(10)

USDJPY_hour_dist = USDJPY_hour.groupBy("hour").count().orderBy("hour", ascending=True)
USDJPY_hour_dist.show(24)

USDJPY_Hour = USDJPY_hour_dist.toPandas()
USDJPY_Hour['count'] = USDJPY_Hour['count'] / day
fig1 = USDJPY_Hour.plot('hour', 'count', kind='bar', figsize=(10, 6), title='USDJPY Activity During a Day')
fig1.set_xticklabels(labels=USDJPY_Hour['hour'], rotation=0)
fig1.set_xlabel('Hour')
fig1
fig1.get_figure().savefig('USDJPY1.jpg', dpi=100, bbox_inches='tight')


def toweekday(utc):
    utc = utc.split('T')[0]
    weekday = datetime.strptime(utc, '%Y-%m-%d').strftime('%w')
    return weekday


udf_toweekday = udf(toweekday)

USDJPY_weekday = spark.sql("SELECT * FROM USDJPY ORDER BY time ASC")
USDJPY_weekday = USDJPY_weekday.withColumn('weekday', udf_toweekday('time'))
USDJPY_weekday.show(10)
USDJPY_weekday_dist = USDJPY_weekday.groupBy("weekday").count().orderBy("weekday", ascending=True)
USDJPY_weekday_dist.show()

USDJPY_Weekday = USDJPY_weekday_dist.toPandas()
USDJPY_Weekday['count'] = USDJPY_Weekday['count'] / week
fig2 = USDJPY_Weekday.plot('weekday', 'count', kind='bar', figsize=(8, 6), title='USDJPY Activity During a Week')
fig2.set_xticklabels(labels=['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri'], rotation=0)
fig2.set_xlabel('Weekday')
fig2.get_figure().savefig('USDJPY2.jpg', dpi=100, bbox_inches='tight')


conn = boto.connect_s3(*,
                       *,
                       host='s3.amazonaws.com')
bucket = conn.get_bucket("strenuousfx")

output_pic = bucket.new_key('USDJPY1.jpg')
output_pic.content_type = 'image/png'
output_pic.set_contents_from_filename('USDJPY1.jpg', policy='public-read')



GBP_JPY = raw.filter(raw["instrument"] == "GBP_JPY").selectExpr("time","instrument","closeoutBid", "closeoutAsk")
GBP_JPY.cache()
GBP_JPY.head(10)
GBP_JPY.write.parquet("s3a://strenuousfx/output/GBP_JPY")
GBPJPY = spark.read.parquet("s3a://strenuousfx/output/GBP_JPY")

GBPJPY.createOrReplaceTempView("GBPJPY")

GBPJPY_hour = spark.sql("SELECT * FROM GBPJPY ORDER BY time ASC")
GBPJPY_hour = GBPJPY_hour.withColumn('hour', split(split('time', 'T')[1], ':')[0])
GBPJPY_hour_dist = GBPJPY_hour.groupBy("hour").count().orderBy("hour", ascending=True)

GBPJPY_Hour = GBPJPY_hour_dist.toPandas()
GBPJPY_Hour['count'] = GBPJPY_Hour['count'] / day
fig1 = GBPJPY_Hour.plot('hour', 'count', kind='bar', figsize=(10, 6), title='GBPJPY Activity During a Day')
fig1.set_xticklabels(labels=GBPJPY_Hour['hour'], rotation=0)
fig1.set_xlabel('Hour')
fig1.get_figure().savefig('GBPJPY1.jpg', dpi=100, bbox_inches='tight')

GBPJPY_weekday = spark.sql("SELECT * FROM GBPJPY ORDER BY time ASC")
GBPJPY_weekday = GBPJPY_weekday.withColumn('weekday', udf_toweekday('time'))
GBPJPY_weekday_dist = GBPJPY_weekday.groupBy("weekday").count().orderBy("weekday", ascending=True)

GBPJPY_Weekday = GBPJPY_weekday_dist.toPandas()
GBPJPY_Weekday['count'] = GBPJPY_Weekday['count'] / week
fig2 = GBPJPY_Weekday.plot('weekday', 'count', kind='bar', figsize=(8, 6), title='GBPJPY Activity During a Week')
fig2.set_xticklabels(labels=['Sun','Mon','Tue','Wed','Thu','Fri'], rotation=0)
fig2.set_xlabel('Weekday')
fig2.get_figure().savefig('GBPJPY2.jpg', dpi=100, bbox_inches='tight')

output_pic = bucket.new_key('GBPJPY1.jpg')
output_pic.content_type = 'image/png'
output_pic.set_contents_from_filename('GBPJPY1.jpg', policy='public-read')

output_pic = bucket.new_key('GBPJPY2.jpg')
output_pic.content_type = 'image/png'
output_pic.set_contents_from_filename('GBPJPY2.jpg', policy='public-read')

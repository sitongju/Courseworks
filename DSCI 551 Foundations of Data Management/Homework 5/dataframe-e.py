from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

spark = SparkSession\
    .builder\
    .appName('readfile')\
    .getOrCreate()

country = spark.read.json('country.json')
country.groupBy('Continent').agg(fc.mean('LifeExpectancy').alias('avg_le'),
fc.count('*').alias('cnt')).filter('cnt >= 20').orderBy(fc.desc('cnt')).select('Continent','avg_le').limit(1).show()

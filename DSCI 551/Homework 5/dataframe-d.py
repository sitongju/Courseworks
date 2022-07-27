from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName('readfile')\
    .getOrCreate()

cl = spark.read.json('countrylanguage.json')
cl[cl.CountryCode == 'CAN'][['Language']].show()


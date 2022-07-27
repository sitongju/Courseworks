from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName('readfile')\
    .getOrCreate()

country = spark.read.json('country.json')
city = spark.read.json('city.json')
country.join(city, country.Capital == city.ID).select(country['Name'], city['Name']).show()

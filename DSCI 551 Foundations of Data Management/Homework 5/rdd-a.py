from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName('readfile')\
    .getOrCreate()

country = spark.read.json('country.json')

result = country.rdd.filter(lambda r: r['Continent']=='North America').map(lambda r:r['Name']).collect()
for i in result:
    print(i)

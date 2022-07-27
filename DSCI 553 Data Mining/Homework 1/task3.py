"""
Sitong Ju
DSCI 552
Homework 1 Task 3
sitongju@usc.edu

A. What is the average stars for each city? (1 point)
B. You are required to compare the execution time of using two methods to print top 10 cities with highest stars.
"""
import sys
import time
import json
from pyspark import SparkContext, SparkConf

sc = SparkContext()
args = sys.argv[:]

rdd1 = sc.textFile(args[2]).map(lambda row: json.loads(row))
rdd2 = sc.textFile(args[1]).map(lambda row: json.loads(row))

#step 1 output from test_review: (business_id, number of reviews, total stars)
rdd_temp1 = rdd2.map(lambda r: (r['business_id'],(1, r['stars']))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd_temp2 = rdd1.map(lambda r: (r['business_id'],r['city']))
rdd_temp3 = rdd_temp2.join(rdd_temp1).map(lambda r: r[1]).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda r:(r[0],r[1][1]/r[1][0])).sortBy(lambda r: (-r[1], r[0]),ascending = True).collect()
#print(rdd_temp3)

f= open(args[3],'w')
f.write('city,stars\n')
for i in rdd_temp3:
    f.writelines([i[0],',',str(i[1]),'\n'])
f.close()

unsorted_python = rdd_temp2.join(rdd_temp1).map(lambda r: r[1]).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda r:(r[0],r[1][1]/r[1][0])).collect()
unsorted_spark = sc.parallelize(unsorted_python)

#method1 using python
start = time.clock()
python_sorted = sorted(unsorted_python, key=lambda r: (-r[1], r[0]))
end = time.clock()
for i in range(1,11):
    print(python_sorted[i])

#method2 using spark
start1 = time.clock()
spark_sorted = unsorted_spark.sortBy(lambda r: (-r[1], r[0]),ascending = True).take(10)
print(spark_sorted)
end1 = time.clock()

result_3b = {
    "m1": end-start,
    "m2": end1-start1,
    'reason': 'Currently on my own computer, python is faster than spark, because the size of the files are limited. '
              'Spark will have advantage when dealing with a large amount of data'
}
with open(args[4], 'w', encoding='utf-8') as f:
    json.dump(result_3b, f,indent=4)
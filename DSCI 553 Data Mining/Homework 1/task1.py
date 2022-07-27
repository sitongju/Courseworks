"""
Sitong Ju
DSCI 552
Homework 1 Task 1
sitongju@usc.edu

A. The total number of reviews (0.5 point)
B. The number of reviews in 2018 (0.5 point)
C. The number of distinct users who wrote reviews (0.5 point)
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote (0.5 point)
E. The number of distinct businesses that have been reviewed (0.5 point)
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had (0.5 point)
"""
import sys
import json
from pyspark import SparkContext, SparkConf

args = sys.argv[:]

sc = SparkContext()
rdd = sc.textFile(args[1]).map(lambda row: json.loads(row))
#test_review.json
result_a = rdd.count()
result_b = rdd.filter(lambda r: r['date']>= '2018-01-01' and r['date'] < '2019-01-01').count()
result_c = rdd.map(lambda r: r['user_id']).distinct().count()
result_d = rdd.map(lambda r: (r['user_id'],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda r: (-r[1], r[0]),ascending = True).take(10)
result_e = rdd.map(lambda r: r['business_id']).distinct().count()
result_f = rdd.map(lambda r: (r['business_id'],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda r: (-r[1], r[0]),ascending = True).take(10)

d_list = []
for i in result_d:
    d_list.append([i[0], i[1]])

f_list = []
for i in result_f:
    f_list.append([i[0], i[1]])


result_dict = {
    'n_review':result_a,
    'n_review_2018':result_b,
    'n_user': result_c,
    'top10_user':d_list,
    'n_business':result_e,
    'top10_business': f_list
}

with open(args[2], 'w', encoding='utf-8') as f:
    json.dump(result_dict, f,indent=4)
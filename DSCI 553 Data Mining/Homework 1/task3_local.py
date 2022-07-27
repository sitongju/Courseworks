# -*- coding: utf-8 -*-
"""
Created on Sun Sep 19 13:04:30 2021

@author: 13451
"""


from pyspark import SparkContext
import json
import time

sc = SparkContext(master='local[*]',appName='AS1T3')
filepath1 = "jiahaosu@usc.edu_resource_1575654_s1575654_516245_Sep_18_2021_11-40-10am_PDT\
/resource/asnlib/publicdata/test_review.json"
filepath2 = "jiahaosu@usc.edu_resource_1575654_s1575654_516245_Sep_18_2021_11-39-55am_PDT\
/resource/asnlib/publicdata/business.json"

review_raw = sc.textFile('test_review.json').map(lambda row:json.loads(row))
business_raw = sc.textFile('business.json').map(lambda row:json.loads(row))

#A
review = review_raw.map(lambda r:(r['business_id'],r['stars']))
business = business_raw.map(lambda r:(r['business_id'],r['city']))
r_b = business.join(review).map(lambda r:(r[1][0],(r[1][1],1)))\
        .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
        .map(lambda r:(r[0],r[1][0]/r[1][1]))\
        .sortBy(lambda row:(-row[1],row[0]),ascending =True).collect()
"""
f = open('output3a.txt','a')
f.write('city,stars\n')
for items in r_b:
    f.writelines([items[0],',',str(items[1]),'\n'])
f.close()
"""


'''
f1 = open(filepath1,'r',encoding = 'utf-8')
content1 = []
for line in f1.readlines():
    dic = json.loads(line)
    content1.append([dic['business_id'],dic['stars']])

f2 = open(filepath2,'r',encoding = 'utf-8')
content2=[]
for line in f2.readlines():
    dic = json.loads(line)
    content2.append([dic['business_id'],dic['city']])

df_r = pd.DataFrame(content1,columns=['business_id','stars'])
df_b = pd.DataFrame(content2,columns=['business_id','city'])
df = pd.merge(df_b,df_r,on = 'business_id')
result = df.groupby(['city'])[['stars']].mean().\
        sort_values(by=['stars','city'],ascending = (False,True))[:10]
'''

#B
#use python sort
data = business.join(review).map(lambda r:(r[1][0],(r[1][1],1)))\
        .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
        .map(lambda r:(r[0],r[1][0]/r[1][1])).collect()
start_1 = time.clock()
res1 = sorted(data,key = lambda x:(-x[1],x[0]))[:10]
for item in res1:
    print(item)
m1 = time.clock()-start_1

#use pyspark rdd sort
start_2 = time.clock()
data_rdd = sc.parallelize(data)
res2 = data_rdd.sortBy(lambda row:(-row[1],row[0]),ascending =True).take(10)
for item in res2:
    print(item)       
m2 = time.clock()-start_2
print(m1)
print(m2)
"""
ans = {"m1":m1,"m2":m2,"reason":"When dealing with small data, just like handling the subset of the data in my local environment,\
       python is faster than spark. But when it comes to big data,spark has better efficiency"}
file_name = "output3b.json"
with open(file_name,'w') as f:
    json.dump(ans, f, indent = 4)

"""
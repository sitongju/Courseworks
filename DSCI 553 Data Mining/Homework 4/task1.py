from graphframes import GraphFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import sys
import itertools

args = sys.argv

filter_threshold = args[1]
input_file = args[2]
output_file = args[3]

#filter_threshold = 7
#input_file = 'ub_sample_data.csv'
#output_file = 'output1.txt'

sc = SparkContext()
sc.setLogLevel("ERROR")

sqlContext = SQLContext(sc)

df = sqlContext.read.format('csv').options(header='true', inferschema='true').csv(input_file)
dfrdd = df.rdd

user = dfrdd.map(lambda r: r[0]).distinct().zipWithIndex()
user_index_dict = user.map(lambda r: (r[0], r[1])).collectAsMap()
index_user_dict = user.map(lambda r: (r[1], r[0])).collectAsMap()

business = dfrdd.map(lambda r: r[1]).distinct().zipWithIndex()
business_index_dict = business.collectAsMap()
#print(business_index_dict)

user_business = dfrdd.map(lambda r: (user_index_dict[r[0]], business_index_dict[r[1]])).groupByKey().mapValues(set)
user_business_dict = user_business.collectAsMap()
#print(user_business_dict)

user_pairs = list(itertools.combinations(list(user_business_dict.keys()), 2))
edges = []
nodes = set()

for user_pair in user_pairs:
    set_user0 = set(user_business_dict[user_pair[0]])
    set_user1 = set(user_business_dict[user_pair[1]])
    if len(set_user0.intersection(set_user1)) >= int(filter_threshold):
        edges.append(tuple((user_pair[0], user_pair[1])))
        edges.append(tuple((user_pair[1], user_pair[0])))
        nodes.add(user_pair[0])
        nodes.add(user_pair[1])

nodes_df = sc.parallelize(list(nodes)).distinct().map(lambda r: (r,)).toDF(['id'])
edges_df = sc.parallelize(edges).toDF(["src", "dst"])

gf = GraphFrame(nodes_df, edges_df)
result_df = gf.labelPropagation(maxIter=5)

result_rdd = result_df.rdd.coalesce(1)\
        .map(lambda r: (r[1], r[0])).groupByKey()\
        .map(lambda r: list(r[1]))\
    .map(lambda r: [index_user_dict[i] for i in r])\
    .map(lambda r: sorted(list(r))) \
    .sortBy(lambda r: (len(r), r)).collect()

with open(output_file, 'w') as f:
    for row in result_rdd:
        f.writelines(str(row)[1:-1] + "\n")







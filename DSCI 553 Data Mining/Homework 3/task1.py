from pyspark import SparkContext
import time
import itertools
import sys

args = sys.argv[:]
sc = SparkContext()

start = time.time()
rdd = sc.textFile(args[1])
header = rdd.first()
raw_data = rdd.filter(lambda r: r!= header).map(lambda r: r.split(','))
#print(num_users)
#user_id = raw_data.map(lambda r : r[1]).zipWithIndex().collectAsMap()
#print(indices)

users = raw_data.map(lambda r: r[0]).distinct().collect()
users.sort()

map_user = {}
for i, u in enumerate(users):
    map_user[u] = i
num_users = len(map_user)

matrix = raw_data.map(lambda r: (r[1], map_user[r[0]])).groupByKey().mapValues(list).sortBy(lambda r: r[0])#.mapValues(lambda r: set(r))#.collectAsMap()
#map(lambda r: (r[0], list(r[1])))
#mapValues(list)
matrix_map = matrix.collectAsMap()
#print(matrix_map)

#num_users = raw_data.map(lambda r: r[0]).distinct().count()
hash_num = 80
band = 40
row = 2

def hashing(users):

    hashed_list = []
    for i in range(0, hash_num):
        code_list = []
        for user in users:
            code = ((i**2)*user + (5*(i+1)+13)) % num_users
            code_list.append(code)
        hashed_list.append(min(code_list))

    return hashed_list

hashed = matrix.mapValues(lambda r: hashing(list(r)))#.collect()
#print(hashed.collect())

"""
def minhash(row,a,b):
    #global num_users
    hash_list = []
    for i in row:
        hval = (a * i + b) % num_users
        hash_list.append(hval)
    return min(hash_list)

hashed = matrix.map(lambda r:(r[0],[minhash(r[1],a,b) for (a,b) in random_coef]))
#print(hashed.collect())
"""

def lsh(x, band, row):

    business = x[0]
    signatures = x[1]

    signature_tuples = []
    for band in range(0,band):
        sig = [band] + signatures[band * row:(band * row) + row]
        signature_tuples.append((tuple(sig), business))
    #print(signature_tuples)
    return signature_tuples

def get_pairs(x):
    return sorted(list(itertools.combinations(sorted(x), 2)))

candidates = hashed.flatMap(lambda r: lsh(r, band, row)).groupByKey().mapValues(list).filter(lambda x: len(x[1]) > 1)\
    .flatMap(lambda x : get_pairs(sorted(x[1]))).distinct()#.collect()

def jaccard(r):
    #global matrix_map

    business1 = r[0]
    business2 = r[1]

    c1 = set(matrix_map[business1])
    c2 = set(matrix_map[business2])
    intersect = len(c1.intersection(c2))
    union = len(c1.union(c2))
    sim = intersect/union
    #print(business1, business2, sim)

    return business1, business2, sim

required_candidates = candidates.map(lambda r: jaccard(r)).filter(lambda r: r[2] >= 0.5).sortBy(lambda r: (r[0], r[1])).collect()

with open(args[2], 'w') as f:
    f.write("business_id_1, business_id_2, similarity\n")
    for line in required_candidates:
        f.write(str(line[0]) + "," + str(line[1]) + "," + str(line[2]) + "\n")
#print(len(required_candidates))
end = time.time()
print(end - start)
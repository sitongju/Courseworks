"""
Sitong Ju
DSCI 552
Homework 2 Task 1
sitongju@usc.edu

"""
import sys
from pyspark import SparkContext
import itertools
import time

args = sys.argv[:]
case_num = int(args[1])
whole_support = int(args[2])

sc = SparkContext()

start = time.time()


if case_num == 1:
    rdd = sc.textFile(args[3]).map(lambda row: (row.split(',')[0],row.split(',')[1]))
    rdd1 = rdd.filter(lambda r: r[0] != 'user_id').groupByKey()\
                .mapValues(set)#.repartition(4)

elif case_num == 2:
    rdd = sc.textFile(args[3]).map(lambda row: (row.split(',')[1], row.split(',')[0]))
    rdd1 = rdd.filter(lambda r: r[0] != 'business_id').groupByKey()\
            .mapValues(set)#.repartition(4)

total_baskets_num = rdd1.count()

begin_apriori = time.time()
#print('begin apriori',begin_apriori - start)
def apriori(iterator):
    partlen = 0
    baskets = []
    candidates = {}

    for row in iterator:
        baskets.append(row[1])
        partlen += 1
        for item in row[1]:
            if item not in candidates:
                candidates[item] = 1
            else:
                candidates[item] += 1

    support = (partlen/total_baskets_num) * whole_support

    """
    for basket in baskets:
        for item in basket:
            if item not in whole_candidates.keys():
                whole_candidates[item] = 1
            else:
                whole_candidates[item] += 1
    """
    '''
    for key in list(candidates.keys()):
        if candidates[key] < support:
            del candidates[key]
    '''
    whole_candidates = []
    for key, value in candidates.items():
        if value >= support:
            whole_candidates.append(key)

    stop = False
    s = 2

    candidate_last_level = sorted(set(whole_candidates)) # single candidates for now
    # has to use set otherwise it's too slow

    #print('before while loop', time.time() - begin_apriori)
    while stop == False:

        curr_level = []
        curr_dict = {}

        if s == 2:
            for item in itertools.combinations(candidate_last_level,s):
                for basket in baskets:
                    if set(item) <= set(basket):
                        if item not in curr_dict.keys():
                            curr_dict[item] = 1
                        else:
                            curr_dict[item] += 1

            #print('after s = 2', time.time() - begin_apriori)

        else:
            for item in itertools.combinations(candidate_last_level, 2):
                if len(set(item[0]).intersection(set(item[1]))) == s - 2:
                    candidate = tuple(sorted(set(item[0]).union(set(item[1]))))
                    if candidate in curr_level:
                        continue
                    else:
                        check_if_qualify = itertools.combinations(candidate, s-1)
                        if set(check_if_qualify).issubset(candidate_last_level):
                            curr_level.append(candidate)

            for item in curr_level:
                for basket in baskets:
                    if set(item) <= set(basket):
                        if item not in curr_dict.keys():
                            curr_dict[item] = 1
                        else:
                            curr_dict[item] += 1

            #print('after else', time.time() - begin_apriori)

        #whole_candidates = whole_candidates + list(curr_dict.keys())

        candidate_last_level = set()
        for key,value in curr_dict.items():
            if value >= support:
                candidate_last_level.add(key)

        whole_candidates = whole_candidates + list(candidate_last_level)
        #print('after update layer', time.time() - begin_apriori)

        if len(candidate_last_level) == 0:
            stop = True
        else:
            s+=1

    yield whole_candidates

def reduce_lists(x,y):
    return list(set(x).union(set(y)))

candidate_list = rdd1.mapPartitions(apriori).reduce(reduce_lists)

#print('after apriori',time.time() - begin_apriori)

def find_true_frequent(iterator):

    countTable = {}
    for i in iterator:
        for candidate in candidate_list:
            if isinstance(candidate, str):
                if set([candidate]) <= set(i[1]):
                    countTable.setdefault(candidate, 0)
                    countTable[candidate] += 1
            else:
                if set(candidate) <= set(i[1]):
                    countTable.setdefault(candidate, 0)
                    countTable[candidate] += 1
    ans_list = []
    for key in countTable:
        ans_list.append((key, countTable[key]))
    return ans_list

def print_items(itemlist):
    print_dict = dict()
    for i in itemlist:
        if isinstance(i, str):
            print_dict.setdefault(1,[])
            print_dict[1].append("('"+i+"')")
        else:
            print_dict.setdefault(len(i),[])
            print_dict[len(i)].append(str(i))
    return print_dict

frequent_list = rdd1.mapPartitions(find_true_frequent).reduceByKey(lambda x,y: x+y)\
    .filter(lambda r:r[1] >= whole_support).map(lambda r:r[0]).collect()

f = open(args[4], 'w')
f.write('Candidates:'+ '\n')
i = 1
#print(print_items(candidate_list))
while i in print_items(candidate_list).keys():
    f.write(','.join(sorted(print_items(candidate_list)[i])))
    f.write('\n')
    f.write('\n')
    i += 1

f.write('Frequent Itemsets:'+'\n')
i = 1
while i in print_items(frequent_list).keys():
    f.write(','.join(sorted(print_items(frequent_list)[i])))
    f.write('\n')
    f.write('\n')
    i += 1
f.close()

end = time.time()

#print(len(candidate_list))
#print(len(frequent_list))

print('Duration:',str(end - start))
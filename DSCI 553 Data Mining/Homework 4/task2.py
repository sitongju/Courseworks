import itertools
import sys
import time
from pyspark import SparkContext
import collections
import random

args = sys.argv

#filter_threshold = args[1]
#input_path = args[2]
#betweenness_file = args[3]
#community_file = args[4]

filter_threshold = 7
input_path = 'ub_sample_data.csv'
betweenness_file = 'output2.txt'
community_file = 'output3.txt'

sc = SparkContext()

start = time.time()

data_rdd = sc.textFile(input_path)
header = data_rdd.first()
user_business_dict = data_rdd.filter(lambda r: r != header).map(lambda r: (r.split(',')[0], r.split(',')[1])) \
        .groupByKey().mapValues(lambda r: sorted(list(r))).collectAsMap()

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

#print(edges)
nodes_rdd = sc.parallelize(sorted(list(nodes))).map(lambda r: (r, 1))
edges_dict = sc.parallelize(edges).groupByKey().mapValues(lambda r: sorted(list(set(r)))).collectAsMap()
unsorted_edges_dict = sc.parallelize(edges).groupByKey().mapValues(lambda r: list(r)).collectAsMap()

def build_tree(rootmap):
    root = list(rootmap)[0]

    tree = dict()
    tree[root] = (0, list())

    visited_nodes = set()
    need_search = list()
    need_search.append(root)

    while len(need_search) > 0:
        parent = need_search.pop(0)
        visited_nodes.add(parent)
        for children in edges_dict[parent]:
            if children not in visited_nodes:
                visited_nodes.add(children)
                tree[children] = (tree[parent][0] + 1, [parent])
                need_search.append(children)
            elif children in tree and tree[parent][0] + 1 == tree[children][0]:
                # the children may have multiple parents
                tree[children][1].append(parent)

    tree = {k: v for k, v in sorted(tree.items(), key=lambda r: -r[1][0])}
    return tree

def Girvan_Newman(tree):
    # tree: {children: (level, parent), ...}
    score_dict = collections.defaultdict(dict) #{parent: {children1: score, children2: score}}
    score_tuple_lst = []
    for k,v in tree.items():
        if len(v[1]) == 1: #the children only has one parent
            if k not in score_dict.keys(): # the children does not have other children
                score_dict[v[1][0]][k] = 1
            else: # the children does have children
                values_total = 0
                for i in score_dict[k].values():
                    values_total += float(i)
                #print(values_total)
                score_dict[v[1][0]][k] = 1+values_total
        elif len(v[1]) > 1: # the node has multiple parents
            node_score = 1
            if k in score_dict.keys(): # if the node has children,
                for i in score_dict[k].values():
                    node_score += float(i)

            shortest_path_lst = []
            for parent in v[1]:
                num_shortest_path = len(tree[parent][1])
                shortest_path_lst.append(num_shortest_path)
            denominator = 0
            for num in shortest_path_lst:
                denominator += float(num)
            for i in range(len(shortest_path_lst)):
                score_dict[v[1][i]][k] = (float(shortest_path_lst[i])/denominator) * node_score

    for k,v in score_dict.items():
        for i,j in v.items():
            edge = (k, i)
            edge = tuple(sorted(edge))
            score_tuple_lst.append((edge, j))

    #score_tuple_lst.sort(key = lambda r: (-r[1],r[0][1]))
    return score_tuple_lst

#print(Girvan_Newman(tree_sample))

# task 2.1 compute betweenness
betweenness_result = nodes_rdd.map(build_tree).flatMap(Girvan_Newman).reduceByKey(lambda x,y: x+y)\
    .map(lambda r: (r[0], round(r[1]/2, 5))).sortBy(lambda r: (-r[1], r[0])).collect()

with open(betweenness_file, 'w+') as output_file:
    for i in betweenness_result:
        output_file.writelines(str(i[0]) + ','+ str(i[1]) + "\n")
    output_file.close()

def detect_community(node, edges_dict):

    visited_nodes = set()
    community = set()
    near_nodes = edges_dict[node]
    while True:
        visited_nodes = visited_nodes.union(near_nodes)
        new_nodes = set()
        for n in near_nodes:
            new_near_nodes = edges_dict[n]
            new_nodes = new_nodes.union(new_near_nodes)
        new_visited_nodes = visited_nodes.union(new_nodes)

        if len(visited_nodes) == len(new_visited_nodes):
            break
        near_nodes = new_nodes - visited_nodes

    community = visited_nodes
    if community == set():
        community = {node}
    return community

def find_communities(node, nodes, edges_dict):

    communities = []
    visited_nodes = detect_community(node, edges_dict)
    unvisited_nodes = nodes - visited_nodes
    communities.append(visited_nodes)
    while True:
        just_visited_nodes = detect_community(random.sample(unvisited_nodes, 1)[0], edges_dict)
        communities.append(just_visited_nodes)
        visited_nodes = visited_nodes.union(just_visited_nodes)
        unvisited_nodes = nodes - visited_nodes
        if len(unvisited_nodes) == 0:
            break

    return communities

def calculate_modularity(communities, m):

    modularity = 0
    for comm in communities:
        part_mod = 0
        nodes_list = list(itertools.permutations(list(comm), 2))
        # permunations! not combinations.
        for i in nodes_list:
            part_mod += A[i] - degree[i[0]] * degree[i[1]] / (2 * m)
        modularity += part_mod
    modularity = modularity/(2 * m)

    return modularity

# task 2.2 find communities
degree = dict()
for k,v in edges_dict.items():
    degree[k] = len(v)

A = dict() # adjacent matrix mapping; A does not change
nodes_list = list(itertools.permutations(list(nodes), 2))
# permunations! not combinations.
for i in nodes_list:
    if i in edges:
        A[i] = 1
    else:
        A[i] = 0

m = len(edges) / 2 # m does not change
edges_remain = m
communities = []
mod_lst = [0]

ifcontinue = True
while ifcontinue:
    highest_betweenness = betweenness_result[0][1]
    for edge in betweenness_result:
        if edge[1] == highest_betweenness:
            edges_dict[edge[0][0]].remove(edge[0][1])
            edges_dict[edge[0][1]].remove(edge[0][0])
            edges_remain -= 1

    curr_communities = find_communities(random.sample(nodes, 1)[0], nodes, edges_dict)
    curr_mod = calculate_modularity(curr_communities, m)

    max_mod = max(mod_lst)

    if curr_mod > max_mod:
        communities = curr_communities

    mod_lst.append(curr_mod)

    # break conditions
    if edges_remain == 0:
        ifcontinue = False
    #elif len(mod_lst) > 5 and mod_lst[-1]<mod_lst[-2] and mod_lst[-2]<mod_lst[-3] and mod_lst[-3]<mod_lst[-4]\
    #    and mod_lst[-4]<mod_lst[-5]:
        # avoid too long looping. Stop the loop if the modularity is continuously decreasing
        ifcontinue = False

    betweenness_result = sc.parallelize(sorted(list(nodes))).map(lambda r: (r, 1)) \
                        .map(build_tree).flatMap(Girvan_Newman).reduceByKey(lambda x, y: x + y) \
                        .map(lambda r: (r[0], round(r[1]/2, 5))).sortBy(lambda r: (-r[1], r[0])).collect()

sorted_communities = sc.parallelize(communities) \
	.map(lambda r: sorted(r)) \
	.sortBy(lambda r: (len(r), r)).collect()

with open(community_file, 'w+') as f2:
    for community in sorted_communities:
        f2.write(str(community)[1:-1] + '\n')

print(time.time()-start)
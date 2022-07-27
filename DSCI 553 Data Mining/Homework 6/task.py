import math
import sys
from sklearn.cluster import KMeans
from sklearn.metrics import normalized_mutual_info_score
import numpy as np
import time
import itertools

start = time.time()

input_file = 'hw6_clustering.txt'
num_clusters = 10
output_file = 'output.txt'

#input_file = sys.argv[1]
#num_clusters = int(sys.argv[2])
#output_file = sys.argv[3]

# Step 1. Load 20% of the data randomly.
f1 = open(input_file, "r")
data = np.array(f1.readlines())
f1.close()

#sample = np.random.choice(data, int(len(data) * 20/100), replace=False)
#data_remains = np.array([i for i in range(data.shape[0]) if data[i] not in sample])
np.random.shuffle(data)
slice_start = 0
size = int(len(data) * 20/100)
sample = data[slice_start : slice_start + size]     

sample_lst = []
for i in sample:
    line = i.split(',')
    sample_lst.append(line)
sample_arr = np.array(sample_lst).astype(np.float)

def str2float(lst):
    for num in lst:
        num = float(num)
    return lst

sample_lst = list(map(str2float,sample_lst))

first_load_points = sample_arr[:,2:]
#first_load_index = sample_arr[:,0]

d = len(sample_arr[0])-2
threshold_distance = 2 * math.sqrt(d)

# Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters)
# on the data in memory using the Euclidean distance as the similarity measurement.

def create_cluster_dict(clusters, data):
    clusters_dict = {}
    for i in range(len(clusters)):
        point = data[i]
        clusterid = clusters[i]
        if clusterid in clusters_dict:
            clusters_dict[int(clusterid)].append(point)
        else:
            clusters_dict[int(clusterid)] = [point]
    return clusters_dict
            
kmeans = KMeans(n_clusters = 5 * num_clusters, random_state=0)
clusters = kmeans.fit_predict(first_load_points)
cluster_dict = create_cluster_dict(clusters, sample_lst)


# Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS (outliers).

RS_dict = {}
for key in cluster_dict.keys():
    if len(cluster_dict[key]) == 1:
        point = cluster_dict[key][0]
        RS_dict[point[0]] = point
        sample_lst.remove(point)


# Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.
second_load_points = np.array(sample_lst)[:,2:]
kmeans = KMeans(n_clusters = num_clusters, random_state = 0)
clusters = kmeans.fit_predict(second_load_points)
clusters_dict = create_cluster_dict(clusters, sample_lst)


# Step 5. Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).
# Dictonary struct - cluster_num: [[points list], n, sum, sum_sq, centroid ]
    
DS_dict = {}

for key in clusters_dict.keys():
    DS_dict[key] = []
    points_lst = []
    for point in clusters_dict[key]:
        points_lst.append(point[0])
    DS_dict[key].append(points_lst)
    DS_dict[key].append(len(points_lst))
    DS_dict[key].append(np.sum(np.array(clusters_dict[key])[:,2:].astype(np.float), axis=0))
    DS_dict[key].append(np.sum(np.square(np.array(clusters_dict[key])[:,2:].astype(np.float)), axis=0))
    DS_dict[key].append(np.sum(np.array(clusters_dict[key])[:,2:].astype(np.float), axis=0)/len(points_lst))
    
# The initialization of DS has finished, so far, you have K numbers of DS clusters (from Step 5) and some numbers of RS (from Step 3).

# Step 6. Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS 
# (clusters with more than one points) and RS (clusters with only one point).

retained_set = []
for key in RS_dict.keys():
    retained_set.append(RS_dict[key])

RS_array = np.array(retained_set)[:,2:]
num_cluster = int(len(retained_set) / 2 + 1)
kmeans = KMeans(n_clusters = num_cluster)
clusters = kmeans.fit_predict(RS_array)
CS_clusters_dict = create_cluster_dict(clusters, retained_set)

CS_dict = {}
# Dictonary struct - cluster_num: [[points list], n, sum, sum_sq, centroid ]
for key in CS_clusters_dict.keys():
    if len(CS_clusters_dict[key]) > 1:
        CS_dict[key] = []
        point_lst =[]
        for point in CS_clusters_dict[key]:
            point_lst.append(point[0])
            del RS_dict[point[0]]
        CS_dict[key].append(point_lst)
        CS_dict[key].append(len(point_lst))
        CS_dict[key].append(np.sum(np.array(CS_clusters_dict[key])[:,2:].astype(np.float), axis=0))
        CS_dict[key].append(np.sum(np.square(np.array(CS_clusters_dict[key])[:,2:].astype(np.float)), axis=0))
        CS_dict[key].append(np.sum(np.array(CS_clusters_dict[key])[:,2:].astype(np.float), axis=0)/len(point_lst))

def output_intermediate(f, round_num):
    DS_num = 0
    CS_clusters_num = 0
    CS_num = 0
    for key in DS_dict.keys():
        DS_num += DS_dict[key][1]
    for key in CS_dict.keys():
        CS_clusters_num +=1
        CS_num += CS_dict[key][1]
    RS_num = len(RS_dict)
    f.write('Round ' + str(round_num) + ': ' + str(DS_num) + ',' + str(CS_clusters_num) + ',' + str(
            CS_num) + ',' + str(RS_num) + '\n')
    
f2 = open(output_file, "w")
f2.write('The intermediate results:\n')
round_num =1
output_intermediate(f2, round_num)

# Step 7. Load another 20% of the data randomly.
# Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign
#  them to the nearest DS clusters if the distance is < 2√𝑑.
# Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and assign
#  the points to the nearest CS clusters if the distance is < 2√𝑑
# Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.

def find_clusters_to_merge(CS_dict):
    all_combinations = itertools.combinations(CS_dict.keys(), 2)
    clusters_to_merge = {}
    
    for comb in all_combinations:
        item1 = comb[0]
        item2 = comb[1]
        
        len_1 = CS_dict[item1][1]
        sum_1 = CS_dict[item1][2]
        sumSQ_1 = CS_dict[item1][3]
        centroid_1 = CS_dict[item1][4]
        std_1 = np.sqrt(sumSQ_1/len_1 - np.square(sum_1/len_1))
        
        len_2 = CS_dict[item2][1]
        sum_2 = CS_dict[item2][2]
        sumSQ_2 = CS_dict[item2][3]
        centroid_2 = CS_dict[item2][4]
        std_2 = np.sqrt(sumSQ_2/len_2 - np.square(sum_2/len_2))
        
        m_distance1 = 0
        m_distance2 = 0
        
        for i in range(0, d):
            m_distance1 += ((centroid_1[i] - centroid_2[i]) / std_1[i]) ** 2
            m_distance2 += ((centroid_1[i] - centroid_2[i]) / std_2[i]) ** 2
        m_distance1 = math.sqrt(m_distance1)
        m_distance2 = math.sqrt(m_distance2)
        
        distance = min(m_distance1,m_distance2)
        
        if distance < threshold_distance:
            comb = sorted(comb)
            #print(comb)
            #print(str(comb))
            clusters_to_merge[str(tuple(comb))] = distance
            
    return clusters_to_merge

def find_closest_ds_cluster(key):
    len_1 = CS_dict[key][1]
    sum_1 = CS_dict[key][2]
    sumSQ_1 = CS_dict[key][3]
    centroid_1 = CS_dict[key][4]
    std_1 = np.sqrt(sumSQ_1/len_1 - np.square(sum_1/len_1))
    
    min_distance = float('inf')
    closest_cluster = -1
    for key in DS_dict.keys():
        len_2 = DS_dict[key][1]
        sum_2 = DS_dict[key][2]
        sumSQ_2 = DS_dict[key][3]
        centroid_2 = DS_dict[key][4]
        std_2 = np.sqrt(sumSQ_2/len_2 - np.square(sum_2/len_2))
        
        m_distance1 = 0
        m_distance2 = 0
        
        for i in range(0, d):
            m_distance1 += ((centroid_1[i] - centroid_2[i]) / std_1[i]) ** 2
            m_distance2 += ((centroid_1[i] - centroid_2[i]) / std_2[i]) ** 2
        m_distance1 = math.sqrt(m_distance1)
        m_distance2 = math.sqrt(m_distance2)
        
        distance = min(m_distance1,m_distance2)
        
        if distance <= min_distance:
            min_distance = distance
            closest_cluster = key
    
    return min_distance,closest_cluster


# loop start
while round_num < 5:
    
    slice_start += size
    round_num += 1
    if round_num ==5:
        slice_end = len(data)
    else:
        slice_end = slice_start + size
    sample_new = data[slice_start:slice_end]
    
    sample_lst = []
    for i in sample_new:
        line = i.split(',')
        sample_lst.append(line)
    
    sample_lst = list(map(str2float,sample_lst))
        
    #sample_arr = np.array(sample_lst)
    for i in range(len(sample_lst)):
        newpoint = sample_lst[i][2:]
        newpoint_index = sample_lst[i][0]
        min_distance = float('inf')
        closest_cluster = -1
        for key in DS_dict.keys(): 
            len_ = DS_dict[key][1]
            sum_ = DS_dict[key][2]
            sumSQ = DS_dict[key][3]
            centroid = DS_dict[key][4]
            std = np.sqrt(sumSQ/len_ - np.square(sum_/len_))
        
            m_distance = 0
            for i in range(0, d):
                m_distance += ((float(newpoint[i]) - float(centroid[i])) / float(std[i])) ** 2
            m_distance = math.sqrt(m_distance)

            if m_distance <= min_distance:
                min_distance = m_distance
                closest_cluster = key
    
        # step 8
        if min_distance < threshold_distance:
            DS_dict[closest_cluster][0].append(newpoint_index)
            DS_dict[closest_cluster][1] +=1
            DS_dict[closest_cluster][2] = DS_dict[closest_cluster][2] + np.array(newpoint).astype(np.float)
            DS_dict[closest_cluster][3] = DS_dict[closest_cluster][3] + np.square(np.array(newpoint).astype(np.float))
            DS_dict[closest_cluster][4] = DS_dict[closest_cluster][2]/DS_dict[closest_cluster][1]
    
        # step 9
        else:
            min_distance = float('inf')
            closest_cluster = -1
            for key in CS_dict.keys(): 
                len_ = CS_dict[key][1]
                sum_ = CS_dict[key][2]
                sumSQ = CS_dict[key][3]
                centroid = CS_dict[key][4]
                std = np.sqrt(sumSQ/len_ - np.square(sum_/len_))
        
                m_distance = 0
                for i in range(0, d):
                    m_distance += ((float(newpoint[i]) - float(centroid[i])) / float(std[i])) ** 2
                m_distance = math.sqrt(m_distance)

                if m_distance <= min_distance:
                    min_distance = m_distance
                    closest_cluster = key
        
            if min_distance < threshold_distance:
                CS_dict[closest_cluster][0].append(newpoint_index)
                CS_dict[closest_cluster][1] += 1
                CS_dict[closest_cluster][2] = CS_dict[closest_cluster][2] + np.array(newpoint).astype(np.float)
                CS_dict[closest_cluster][3] = CS_dict[closest_cluster][3] + np.square(np.array(newpoint).astype(np.float))
                CS_dict[closest_cluster][4] = CS_dict[closest_cluster][2]/CS_dict[closest_cluster][1]
        
            # step 10
            else: 
                RS_dict[sample_lst[i][0]] = sample_lst[i]

    # Step 11. Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).

    retained_set_new = []
    for key in RS_dict.keys():
        retained_set_new.append(RS_dict[key])

    RS_array_new = np.array(retained_set_new)[:,2:]
    num_cluster = int(len(retained_set_new) / 2 + 1)
    kmeans = KMeans(n_clusters = num_cluster)
    clusters = kmeans.fit_predict(RS_array_new)
    CS_clusters_dict = create_cluster_dict(clusters, retained_set_new)


    for key in CS_clusters_dict.keys():
        if len(CS_clusters_dict[key]) > 1:
            
            key_list = list(CS_dict.keys())
            new_key = -1
            if len(key_list) != 0:
                max_cluster_num = max(key_list)
                new_key = max_cluster_num+1
            else:
                new_key = 0
                
            CS_dict[new_key] = []
            point_lst =[]
            for point in CS_clusters_dict[key]:
                point_lst.append(point[0])
                del RS_dict[point[0]]
            CS_dict[new_key].append(point_lst)
            CS_dict[new_key].append(len(point_lst))
            CS_dict[new_key].append(np.sum(np.array(CS_clusters_dict[key])[:,2:].astype(np.float), axis=0))
            CS_dict[new_key].append(np.sum(np.square(np.array(CS_clusters_dict[key])[:,2:].astype(np.float)), axis=0))
            CS_dict[new_key].append(np.sum(np.array(CS_clusters_dict[key])[:,2:].astype(np.float), axis=0)/len(point_lst))

    # Step 12. Merge CS clusters that have a Mahalanobis Distance < 2√𝑑.
    wait_to_merge = find_clusters_to_merge(CS_dict)
    # key:(1,2) value: distance

    merged_set = set()
    for k,v in wait_to_merge.items():
        print(k)
        num1 = int(k[1])
        num2 = int(k[4])
        if num1 not in merged_set and num2 not in merged_set:
        
            # merge cs clusters
            CS_dict[num1][0].extend(CS_dict[num2][0])
            CS_dict[num1][1] = CS_dict[num1][1] + CS_dict[num2][1]
            CS_dict[num1][2] = CS_dict[num1][2] + CS_dict[num2][2]
            CS_dict[num1][3] = CS_dict[num1][3] + CS_dict[num2][3]
            CS_dict[num1][4] = CS_dict[num1][2]/CS_dict[num1][1]
            del CS_dict[num2]
        
            merged_set.add(num1)
            merged_set.add(num2)
    
    # If this is the last run (after the last chunk of data), merge CS clusters with DS clusters that have a Mahalanobis Distance < 2√𝑑.
    if round_num ==5: # last round
        CS_keys = list(CS_dict.keys()) 
        for key in CS_keys:
            distance,ds_key = find_closest_ds_cluster(key)
        
            if distance < threshold_distance:
                # merge cs cluster with ds cluster
                DS_dict[ds_key][0].extend(CS_dict[key][0])
                DS_dict[ds_key][1] = DS_dict[ds_key][1] + CS_dict[key][1]
                DS_dict[ds_key][2] = DS_dict[ds_key][2] + CS_dict[key][2]
                DS_dict[ds_key][3] = DS_dict[ds_key][3] + CS_dict[key][3]
                DS_dict[ds_key][4] = DS_dict[ds_key][2]/DS_dict[ds_key][1]
                del CS_dict[key]
    
    output_intermediate(f2, round_num)
    
f2.write('\n')
f2.write('The clustering results: ')
result_lst = []
for key in DS_dict.keys():
    for point in DS_dict[key][0]:
        result_lst.append((int(point),int(key)))
for key in CS_dict.keys():
    for point in CS_dict[key][0]:
        result_lst.append((int(point),-1))
for key in RS_dict.keys():
    result_lst.append((int(key),-1))
    
def sort_helper(r):
    return r[0]

result_lst.sort(key = sort_helper, reverse = False)

for point in result_lst:
    f2.write('\n' + str(int(point[0])) + ',' + str(point[1]))

f2.close()

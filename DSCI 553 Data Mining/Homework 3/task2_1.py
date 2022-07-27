import sys
from pyspark import SparkContext
import time
import math

args = sys.argv[:]
sc = SparkContext()

input_train = args[1]
input_test = args[2]
output_file = args[3]

start_time = time.time()

train = sc.textFile(input_train)
header1 = train.first()
train_data = train.filter(lambda r: r!= header1).map(lambda r: r.split(','))

user_rating = train_data.map(lambda r: ((r[0]), ((r[1]), float(r[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()
#print(user_rating)
business_rating = train_data.map(lambda r: ((r[1]), ((r[0]), float(r[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()
#print(business_rating)
user_rating_avg = train_data.map(lambda r: (r[0], float(r[2]))).groupByKey().mapValues(lambda r: sum(r)/len(r)).collectAsMap()
business_rating_avg = train_data.map(lambda r: (r[1], float(r[2]))).groupByKey().mapValues(lambda r: sum(r)/len(r)).collectAsMap()
overall_avg = train_data.map(lambda r: (1,float(r[2]))).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()
overall_avg_num = overall_avg[0][1]

test = sc.textFile(input_test)
header2 = test.first()
test_data = test.filter(lambda r: r!= header2).map(lambda r: r.split(',')).sortBy(lambda r: ((r[0]), (r[1])))

def pearson_corr(business, neighbor):
    business_avg = business_rating_avg.get(business)    # two numbers
    neighbor_avg = business_rating_avg.get(neighbor)
    business_ratings = business_rating.get(business)    # two maps
    neighbor_ratings = business_rating.get(neighbor)
    
    common_users = []
    for user in business_ratings:
        if user in neighbor_ratings:
            common_users.append(user)

    if len(common_users) == 0: # two business has no common user ratings
        pearson_coefficient = float(business_avg / neighbor_avg)
    else:
        numerator = 0
        denom_business = 0
        denom_neighbor = 0
        for user in common_users:
            business_num = business_ratings.get(user)
            neighbor_num = business_ratings.get(user)
            numerator += (business_num - business_avg) * (neighbor_num - neighbor_avg)
            denom_business += (business_num - business_avg)**2
            denom_neighbor += (neighbor_num - neighbor_avg)**2

        denominator = (math.sqrt(denom_business))* (math.sqrt(denom_neighbor))
        if denominator == 0:
            if numerator == 0:
                pearson_coefficient = 1
            else:
                pearson_coefficient = -1
        else:
            pearson_coefficient = numerator / denominator

    return pearson_coefficient

#print(pearson_corr('fThrN4tfupIGetkrz18JOg', 'uW6UHfONAmm8QttPkbMewQ'))

def get_prediction_num(pearson_corr_list):

    numerator = 0
    denominator = 0
    prediction = 0
    neighbor_cutoff = 50
    pearson_corr_list.sort(key = lambda r: r[0], reverse = True)

    if len(pearson_corr_list) == 0:
        return overall_avg_num
    else:
        true_neighbor = min(len(pearson_corr_list), neighbor_cutoff)
        for i in range(true_neighbor):
            numerator += pearson_corr_list[i][1] * pearson_corr_list[i][0]
            denominator += pearson_corr_list[i][0]
            prediction = numerator/denominator

    if prediction > 5.0:
        prediction = 5.0
    elif prediction < 0.0:
        prediction = 0.0

    return prediction


def item_based_cf(test_data):
    user_id = test_data[0]
    business_id = test_data[1]
    if business_id not in business_rating:
        # business has not been rated, cold start
        if len(list(user_rating.get(user_id))) == 0: 
            # if the user is also new, give an overall avergae num
            return user_id, business_id, str(overall_avg_num)
        else:
            avg_user = user_rating_avg.get(user_id)
            return user_id, business_id, str(avg_user)
    else: # the business has been rated
        avg_business = business_rating_avg.get(business_id)
        if user_id not in user_rating:
            # the business has rating racords, but the user has not rated before
            return user_id, business_id, str(avg_business)
        else: # both the business and the user has rating record
            neighbor_business = list(user_rating.get(user_id))
            pearson_corr_list = []
            for i in neighbor_business:
                pearson_coefficient = pearson_corr(business_id, i)
                current_neighbour_rating = business_rating.get(i).get(user_id)
                if pearson_coefficient > 0:
                    if pearson_coefficient > 1:
                        pearson_coefficient = 1/pearson_coefficient
                    pearson_corr_list.append((pearson_coefficient, current_neighbour_rating))
            prediction_num = get_prediction_num(pearson_corr_list)
            return user_id, business_id, prediction_num

prediction_list = test_data.map(item_based_cf).collect()


f = open(output_file, 'w')
f.write("user_id, business_id, prediction\n")
for i in range(len(prediction_list)):
    f.write(str(prediction_list[i][0]) + "," + str(prediction_list[i][1]) + "," + str(prediction_list[i][2]) + "\n")
f.close()

end_time = time.time()
print("Duration : ", end_time - start_time)

import sys
import json
import pandas as pd
import xgboost as xgb
from pyspark import SparkContext
import time
import math

args = sys.argv[:]
train_files = args[1] #'data/'
test_file = args[2] #'yelp_val.csv'
output_file = args[3] #'output3.csv'

sc = SparkContext()

start = time.time()

# model_based

train_csv = pd.read_csv(train_files+'yelp_train.csv')
test_csv = pd.read_csv(test_file)

user_rdd = sc.textFile(train_files+'user.json').map(lambda r: json.loads(r))\
    .map(lambda r: (r['user_id'], (r['review_count'], r['useful'], r['average_stars'])))
user_map = user_rdd.collectAsMap()

business_rdd = sc.textFile(train_files+'business.json').map(lambda r: json.loads(r))\
    .map(lambda r: (r['business_id'], (r['stars'], r['review_count'])))
business_map = business_rdd.collectAsMap()

user_review_count_avg = user_rdd.map(lambda r: (1, r[1][0])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
user_rating_avg = user_rdd.map(lambda r: (1, r[1][2])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
business_star_avg = business_rdd.map(lambda r: (1, r[1][0])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
business_review_count_avg = business_rdd.map(lambda r: (1, r[1][1])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]

def get_x_dataset(dataset):
    user_review_count = []
    user_useful = []
    user_average_stars = []
    business_stars = []
    business_review_count = []

    x_dataset = pd.DataFrame()
    x_dataset['user_id'] = dataset['user_id']
    x_dataset['business_id'] = dataset['business_id']

    for user in x_dataset['user_id']:
        if user in user_map:
            user_review_count.append(user_map.get(user)[0])
            user_useful.append(user_map.get(user)[1])
            user_average_stars.append(user_map.get(user)[2])
        else:
            user_review_count.append(user_review_count_avg)
            user_useful.append(0)
            user_average_stars.append(user_rating_avg)

    for business in x_dataset['business_id']:
        if business in business_map:
            business_stars.append(business_map.get(business)[0])
            business_review_count.append(business_map.get(business)[1])
        else:
            business_stars.append(business_star_avg)
            business_review_count.append(business_review_count_avg)

    x_dataset['user_review_count'] = user_review_count
    x_dataset['user_useful'] = user_useful
    x_dataset['user_average_stars'] = user_average_stars
    x_dataset['business_stars'] = business_stars
    x_dataset['business_review_count'] = business_review_count

    x_dataset = x_dataset.drop(["user_id"], axis=1)
    x_dataset = x_dataset.drop(["business_id"], axis=1)

    return x_dataset

x_train = get_x_dataset(train_csv)
#print(x_train)
y_train = train_csv.stars.values

x_test = get_x_dataset(test_csv)
y_test = test_csv.stars.values

xgbreg = xgb.XGBRegressor(learning_rate = 0.4)
xgbreg.fit(x_train, y_train)
xgb_result = xgbreg.predict(x_test)

prediction_result = pd.DataFrame()
prediction_result["user_id"] = test_csv.user_id.values
prediction_result["business_id"] = test_csv.business_id.values
prediction_result["prediction"] = xgb_result

prediction_result.to_csv('model_prediction.csv', index = False, sep=',', mode='w')

model_prediction = sc.textFile('model_prediction.csv')
header_prediction = model_prediction.first()
model_prediction_rdd = model_prediction.filter(lambda r: r!= header_prediction).map(lambda r: r.split(',')).map(lambda r: (((r[0]), (r[1])), float(r[2])))

# item_based

train = sc.textFile(train_files+'yelp_train.csv')
header1 = train.first()
train_data = train.filter(lambda r: r!= header1).map(lambda r: r.split(','))

user_rating = train_data.map(lambda r: ((r[0]), ((r[1]), float(r[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()
business_rating = train_data.map(lambda r: ((r[1]), ((r[0]), float(r[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()
user_rating_avg = train_data.map(lambda r: (r[0], float(r[2]))).groupByKey().mapValues(lambda r: sum(r)/len(r)).collectAsMap()
business_rating_avg = train_data.map(lambda r: (r[1], float(r[2]))).groupByKey().mapValues(lambda r: sum(r)/len(r)).collectAsMap()
overall_avg = train_data.map(lambda r: (1,float(r[2]))).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()
overall_avg_num = overall_avg[0][1]

test_rdd = sc.textFile(test_file)
test_header = test_rdd.first()
test_data = test_rdd.filter(lambda x: x != test_header).map(lambda r: r.split(','))

def pearson_corr(business, neighbor):
    business_avg = business_rating_avg.get(business)  # two numbers
    neighbor_avg = business_rating_avg.get(neighbor)
    business_ratings = business_rating.get(business)  # two maps
    neighbor_ratings = business_rating.get(neighbor)

    common_users = []
    for user in business_ratings:
        if user in neighbor_ratings:
            common_users.append(user)

    if len(common_users) == 0:  # two business has no common user ratings
        pearson_coefficient = float(business_avg / neighbor_avg)
    else:
        numerator = 0
        denom_business = 0
        denom_neighbor = 0
        for user in common_users:
            business_num = business_ratings.get(user)
            neighbor_num = business_ratings.get(user)
            numerator += (business_num - business_avg) * (neighbor_num - neighbor_avg)
            denom_business += (business_num - business_avg) ** 2
            denom_neighbor += (neighbor_num - neighbor_avg) ** 2

        denominator = (math.sqrt(denom_business)) * (math.sqrt(denom_neighbor))
        if denominator == 0:
            if numerator == 0:
                pearson_coefficient = 1
            else:
                pearson_coefficient = -1
        else:
            pearson_coefficient = numerator / denominator

    return pearson_coefficient

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

item_based_rdd = test_data.map(item_based_cf).map(lambda r: (((r[0]), (r[1])), float(r[2])))

hybrid_result = item_based_rdd.join(model_prediction_rdd).map(lambda r: ((r[0]), float((r[1][0] * 0.1 + r[1][1] * 0.9)))).collect()

print("Duration : ", time.time() - start)

f = open(output_file, 'w')
f.write("user_id, business_id, prediction\n")
for i in range(len(hybrid_result)):
    f.write(str(hybrid_result[i][0][0]) + "," + str(hybrid_result[i][0][1]) + "," + str(hybrid_result[i][1]) + "\n")
f.close()
import sys
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from pyspark import SparkContext
import time
from sklearn.metrics import mean_squared_error

args = sys.argv[:]
train_files = args[1] #'data/'
test_file = args[2] #'yelp_val.csv'
output_file = args[3] #'output3.csv'

sc = SparkContext()

start = time.time()

train_csv = pd.read_csv(train_files+'yelp_train.csv')
test_csv = pd.read_csv(test_file)

user_rdd = sc.textFile(train_files+'user.json').map(lambda r: json.loads(r))\
    .map(lambda r: (r['user_id'], (r['review_count'], r['useful'], r['average_stars'])))
user_map = user_rdd.collectAsMap()
#print(user_json_rdd)

business_rdd = sc.textFile(train_files+'business.json').map(lambda r: json.loads(r))\
    .map(lambda r: (r['business_id'], (r['stars'], r['review_count'])))
business_map = business_rdd.collectAsMap()
#print(business_json_rdd)

user_review_count_avg = user_rdd.map(lambda r: (1, r[1][0])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
user_rating_avg = user_rdd.map(lambda r: (1, r[1][2])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
business_star_avg = business_rdd.map(lambda r: (1, r[1][0])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
business_review_count_avg = business_rdd.map(lambda r: (1, r[1][1])).groupByKey().mapValues(lambda r: sum(r)/len(r)).collect()[0][1]
#print(user_review_count_avg)
#print(user_rating_avg)
#print(business_star_avg)
#print(business_review_count_avg)

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

prediction_result.to_csv(output_file, header=['user_id', ' business_id', ' prediction'], index = False, sep=',', mode='w')
end = time.time()

print("RMSE:", np.sqrt(mean_squared_error(test_csv.stars.values, xgb_result)))
print('Duration:',str(end-start))
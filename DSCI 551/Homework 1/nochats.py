"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

finds the students who did not have chat messages and their participation locations.
Write output also to a JSON file named nochats.json.

Execution format: python nochats.py <chat-log-file> <roster-file> <output-file>
For example, python nochats.py 551-0125.txt 551-mw-roster.csv nochats.json

Format of your output file:
[{"Name":"David Chen","Participating from":"United States of America"},...]
"""
import json
import pandas as pd

#load txt file and get the data from 1a
f = open('551-0125.txt',mode = "r",encoding = 'UTF-8')
chatdata = f.readlines()
f.close()

chat_dict = dict()
for item in chatdata:
    if 'from' in item:
        split1 = item.split('from ')[1]
        name = split1.split(' to')[0]

    if name not in chat_dict:
        chat_dict[name] = 1
    else:
        chat_dict[name] += 1

#readin the data of the locations of students
csv_data = pd.read_csv('551-mw-roster.csv')
#print(csv_data)

#change the format of Name
new_name = []
for name in csv_data['Name']:
    new_name.append(name.split(',')[1].strip() + ' ' + name.split(',')[0].strip())
csv_data.insert(1,'Name_new',new_name)
del csv_data['Name']

#search the keys in chat_dict

result = []
for i in range(len(csv_data['Name_new'])):
    if csv_data.loc[i,'Name_new']not in chat_dict:
        result.append({"Name":csv_data.loc[i,'Name_new'],"Participating from":csv_data.loc[i,'Participating from']})

#convert to json
json_file = open('nochats.json','w')
ans_json = json.dumps(result)
json.dump(ans_json,json_file)
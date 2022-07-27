"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

convert a given log file into JSON file named roster.json in the format specified below.

Execution format: python convert_chats.py <roster-file> <output-file>
For example, python convert_chats.py 551-mw-roster.csv roster.json
Format of your output file:
[{"Name":"John Smith","Participating from":"United States of America"},...]

"""
import json
import pandas as pd
csv_data = pd.read_csv('551-mw-roster.csv')

#change the format of Name
new_name = []
for name in csv_data['Name']:
    new_name.append(name.split(',')[1].strip() + ' ' + name.split(',')[0].strip())
csv_data.insert(1,'Name_new',new_name)
del csv_data['Name']

#convert into json
result=[]
for i in range(len(csv_data['Name_new'])):
    result.append({"Name":csv_data.loc[i,'Name_new'],"Participating from":csv_data.loc[i,'Participating from']})

#format the output
json_file = open('roster.json','w')
ans_json = json.dumps(result)
json.dump(ans_json,json_file)

"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

convert a given log file into JSON file named
chats.json in the format specified below.

Execution format: python convert_chats.py <chat-log-file> <output-file> For example, python convert_chats.py 551-0125.txt chats.json
Format of your output file:
[{"Time":"4:37PM","Person":"David Chen","Message":"1"},...,]
"""

import json

#load txt file
f = open('551-0125.txt',mode = "r",encoding = 'UTF-8')
chatdata = f.readlines()
f.close()

#build 3 empty lists to put the data
time = []
person = []
message = []
for item in chatdata:
    if 'from' in item:
        time.append(item.split(' ')[4].strip()+item.split(' ')[5].strip())
        split1 = item.split('from ')[1]
        name = split1.split(' to')[0]
        person.append(name)
        message.append(item.split(':')[2].strip())

#build the list of json
ans_json = []
for i in range(len(time)):
    ans_json.append({'Time':time[i],'Person':person[i],'Message':message[i]})

#format the output
json_file = open('chats.json','w')
ans_json = json.dumps(ans_json)
json.dump(ans_json,json_file)

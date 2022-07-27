"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

computes the total number of chats for each person who participated in the chat.
Output the statistics in a JSON file named stats.json.

Execution format: python stats.py <chat-log-file> <output-file>
For example, python stats.py 551-0125.txt stats.json

Format of your output file:
[{"Person":"John Smith","Message":8},...]

"""
import json

#load txt file
f = open('551-0125.txt',mode = "r",encoding = 'UTF-8')
chatdata = f.readlines()
f.close()

#get the name from each line, and put the name into a dictionary
ans_dict = dict()
for item in chatdata:
    if 'from' in item:
        split1 = item.split('from ')[1]
        name = split1.split(' to')[0]

    if name not in ans_dict:
        ans_dict[name] = 1
    else:
        ans_dict[name] += 1

#convert to json
ans_json = []
for key in ans_dict:
    ans_json.append({"Person": key, "Message": ans_dict[key]})

#output
json_file = open('stats.json','w')
ans_json = json.dumps(ans_json)
json.dump(ans_json,json_file)
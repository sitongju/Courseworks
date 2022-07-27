"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

load the JSON data for the chat and roster you generated in
Part 2 into a Firebase database and create any index structure that you need to answer the following questions.

Execution format: python load.py chats.json roster.json

"""
import requests
import json

url = 'https://dsci-551-c55ef-default-rtdb.firebaseio.com/roster.json'

with open('roster.json') as db:
    roster = json.load(db)
    item = str(roster).replace('\'','\"')
    #print(item)
    requests.put(url,str(item))

#chats_put= requests.put('https://dsci-551-c55ef-default-rtdb.firebaseio.com/',roster)


db = open('chats.json')
chats = json.load(db)
url2 = 'https://dsci-551-c55ef-default-rtdb.firebaseio.com/chats.json'
response = requests.put(url2,chats)

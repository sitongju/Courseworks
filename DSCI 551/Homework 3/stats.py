"""
Sitong Ju
DSCI 551 Spring 2021
Homework 3
sitongju@usc.edu

computes the total number of chats for each
person who participated in the chat

"""
import sys
import mysql.connector
import json

args = sys.argv[:]

conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'dsci551')
cur = conn.cursor()

cur.execute('select from_name as Person, count(contents) as Message from chat_log group by from_name')

ans_json = []
for i in cur:
    ans_json.append({"Person": i[0], "Message": i[1]})

conn.commit()
cur.close()
conn.close()

#output
with open(args[1],'w') as f:
    json.dump(ans_json, f, indent = 4)

"""
Sitong Ju
DSCI 551 Spring 2021
Homework 3
sitongju@usc.edu

finds the students who did not have chat messages and their participation locations.

"""
import sys
import mysql.connector
import json

args = sys.argv[:]

conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'dsci551')
cur = conn.cursor()

nochats_sql = """
select 
    name, participating_from
from roster
where name not in (
    select distinct(from_name)
    from chat_log
)
"""
cur.execute(nochats_sql)

ans_json = []
for i in cur:
    ans_json.append({"Name": i[0], "Participating from": i[1]})


conn.commit()
cur.close()
conn.close()

#output
with open(args[1],'w') as f:
    json.dump(ans_json, f, indent = 4)

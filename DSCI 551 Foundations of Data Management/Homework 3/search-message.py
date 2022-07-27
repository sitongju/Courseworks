"""
Sitong Ju
DSCI 551 Spring 2021
Homework 3
sitongju@usc.edu

finds all messages made by a given student.

"""

import sys
import mysql.connector

args = sys.argv[:]

conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'dsci551')
cur = conn.cursor()

search_person_sql = """
select name
from roster
where lower(name) = %s
"""
cur.execute(search_person_sql, (args[1].lower(),))

search_person = []
for i in cur:
    search_person.append(i[0])

if len(search_person) == 0:
    print('Student Not Found')
else:
    search_message_sql = """
    select time, contents
    from chat_log
    where lower(from_name) = %s
    """

    cur.execute(search_message_sql, (args[1].lower(),))

    search_message = []
    for i in cur:
        search_message.append(i[0]+'  '+i[1])

    if len(search_message) == 0:
        print('This student is quiet')
    else:
        for i in search_message:
            print(i)

conn.commit()
cur.close()
conn.close()

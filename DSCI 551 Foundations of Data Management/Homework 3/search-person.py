"""
Sitong Ju
DSCI 551 Spring 2021
Homework 3
sitongju@usc.edu

finds all students whose name contains at least one of the specified keywords (case insensitive)

"""

import sys
import mysql.connector

args = sys.argv[:]

conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'dsci551')
cur = conn.cursor()

list_words = args[1].lower().split(' ')
if len(list_words) == 1:
    one_sql = """
    select name
    from roster
    where lower(name) like %s
    or lower(name) like %s;'
    """
    cur.execute(one_sql, ('% '+list_words[0],list_words[0]+' %'))
elif len(list_words) == 2:
    two_sql = """
    select name
    from roster
    where lower(name) like %s
    or lower(name) like %s
    or lower(name) like %s
    or lower(name) like %s
    """
    cur.execute(two_sql, ('% ' + list_words[0], list_words[0] + ' %','% ' + list_words[1], list_words[1] + ' %'))

result_cur = []
for i in cur:
    result_cur.append(i[0])

if len(result_cur) != 0:
    for i in result_cur:
        print(i)
else:
    print('Student Not Found')

conn.commit()
cur.close()
conn.close()
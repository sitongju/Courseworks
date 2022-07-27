"""
Sitong Ju
DSCI 551 Spring 2021
Homework 3
sitongju@usc.edu

loads the data into mySQL as two tables: Chat and Roster

"""

import sys
import mysql.connector
import pandas as pd

args = sys.argv[:]

conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'dsci551')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS chat_log;')
cur.execute('DROP TABLE IF EXISTS roster;')

create_roster_sql = """
CREATE TABLE roster(
    name varchar(70) PRIMARY KEY,
    participating_from varchar(50)
);
"""
cur.execute(create_roster_sql)
create_chat_log_sql = """
CREATE TABLE chat_log(
    id int PRIMARY KEY AUTO_INCREMENT,
    date varchar(15),
    time varchar(10),
    from_name varchar(70),
    to_name varchar(70),
    contents varchar(200),
    FOREIGN KEY (from_name) REFERENCES roster (name)
);
"""
cur.execute(create_chat_log_sql)

csv_data = pd.read_csv(args[2])

#print(csv_data['Name'])

#change the format of Name
new_name = []
for name in csv_data['Name']:
    new_name.append(name.split(',')[1].strip() + ' ' + name.split(',')[0].strip())
csv_data.insert(1, 'Name_new', new_name)
del csv_data['Name']

for index,row in csv_data.iterrows():
    cur.execute('INSERT INTO roster(name, participating_from) VALUES (%s,%s)',
                (row["Name_new"],row["Participating from"],))

cur.execute('INSERT INTO roster(name,participating_from) values (%s,%s);',('Wensheng Wu', 'United States of America',))

#load txt file
f = open(args[1],mode = "r",encoding = 'UTF-8')
chatdata = f.readlines()
f.close()

#build 3 empty lists to put the data
date = []
time = []
from_name = []
to_name = []
message = []
for item in chatdata:
    if 'from' in item:
        date.append(item.split(' ')[0].strip())
        time.append(item.split(' ')[4].strip()+item.split(' ')[5].strip())
        split1 = item.split('from ')[1]
        name = split1.split(' to')[0]
        from_name.append(name)
        to_name.append(split1.split(' to')[1].split(':')[0])
        message.append(item.split(':')[2].strip())

for i in range(0, len(date)):
    cur.execute('INSERT INTO chat_log(date, time, from_name, to_name, contents) values (%s,%s,%s,%s,%s)',
                (date[i], time[i], from_name[i], to_name[i], message[i],))


conn.commit()
cur.close()
conn.close()
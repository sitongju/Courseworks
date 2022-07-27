"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

finds all messages made by a given student.

"""

import requests

def search_message(name):
    name = name.split(' ')
    name = name[0][0].upper()+name[0][1:].lower()+" "+name[1][0].upper()+name[1][1:].lower()
    url = r'https://dsci-551-c55ef-default-rtdb.firebaseio.com/chats.json?orderBy="Person"&equalTo="' + name + '"'
    response = requests.get(url)

    ans_list = []
    for item in response.json():
        ans_list.append(response.json()[item]['Time'] + ' '+ response.json()[item]['Message'])

    if len(ans_list) != 0:
        for item in ans_list:
            print(item)
    else:
        url2 = r'https://dsci-551-c55ef-default-rtdb.firebaseio.com/roster.json?orderBy="Name"&equalTo="' + name + '"'
        checkstudent = requests.get(url2)
        if len(checkstudent.json()) == 0:
            print("Student Not Found.")
        else:
            print("This student is quiet.")


#search_message('Sitong Ju')

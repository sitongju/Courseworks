"""
Sitong Ju
DSCI 551 Spring 2021
Homework 1
sitongju@usc.edu

finds all students whose name contains at least one of the specified keywords (case insensitive).

"""

import requests

def search_person(name):
    url = 'https://dsci-551-c55ef-default-rtdb.firebaseio.com/roster.json'
    response = requests.get(url)

    #print(type(response.json()))
    list_words = name.lower().split(' ')
    list_names = []
    for item in list_words:
        for lines in response.json():
            names = lines.get('Name').split(' ')
            for name in names:
                if item == name.lower() and lines.get('Name') not in list_names:
                    list_names.append(lines.get('Name'))

    if len(list_names) == 0:
        print("Student Not Found.")
    else:
        for item in list_names:
            print(item)

#search_person('li')

#for i in range(59):
#    url_base =  r'https://dsci551-as1-default-rtdb.firebaseio.com/roster/' + str(i)+ '.json'
#    response = requests.get(url_base)
#    print(response.json())

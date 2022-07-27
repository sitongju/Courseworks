"""
Sitong Ju
DSCI 551 Spring 2021
Homework 2
sitongju@usc.edu

input: input file, index.xml, a list of words
output: return all elements whose text content or attribute contains at least one of the keywords.
For each element, indicate which XML it comes from.

If no such keywords in the file, return “No such tokens”.

"""

from lxml import etree
import argparse

parser = argparse.ArgumentParser(description='')
parser.add_argument('xmlfile',help = 'index.xml')
parser.add_argument('input',help = 'input file')
parser.add_argument('words',help = 'words to search')
args = vars(parser.parse_args())
#print(args)
#print(args["xmlfile"],args["input"],args["words"])

#def search(word):
f = open(args["xmlfile"])
tree = etree.parse(f)
listWords = args["words"].split(' ') # listWords is the list of input words

printSet = set()
for i in listWords:
    #build two lists to hold the which and where of the token. The two lists have same length
    whichList = []
    whereList = []
    #fill in the two lists by search for the token
    for element in tree.xpath('/index/token[value=\''+ i + '\']/provenance/which'):
        #print('which: '+element.text)
        whichList.append(element.text)

    for element in tree.xpath('/index/token[value=\''+ i + '\']/provenance/where'):
        #print('where: '+element.text)
        whereList.append(element.text)

    if len(whichList) == 0: #the token does not exist
        print('No such tokens: '+ i)
    else:
        for j in range(0, len(whichList)): #loop through the address one by one
            #open the xml file based on which list
            g = open(args["input"]+'//'+ whichList[j])
            tree2 = etree.parse(g)
            #get the path of the token from where
            where = whereList[j].split('.')
            path = ''
            #if the last element of the address starts with @, format the path accordingly
            if where[-1][0] == '@':
                for k in range(0, len(where)-1):
                    path += '/' + where[k]
                path = path+'['+where[-1]+'=\''+i+'\']'
            else: #else, format the path accordingly
                for k in where:
                    path += '/'+ k
                path = path+'[contains(.,\''+i+'\')]'

            #print(path)
            for element in tree2.xpath(path):
                #print out the result
                printout = etree.tostring(element).decode('utf‐8').strip()+ '+-+' + whichList[j]
                printSet.add(printout)

for item in printSet:
    print('Element: '+item.split('+-+')[0])
    print('File: '+item.split('+-+')[1])




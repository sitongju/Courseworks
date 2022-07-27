"""
Sitong Ju
DSCI 551 Spring 2021
Homework 2
sitongju@usc.edu

input: "input" directory: a collection of xml
output: index.xml

The index should contain for each token, the token’s value, and all of this token’s provenance.
'which' tag should include the name of the XML file that has the token.
'where' tag should include the path to find the token in relevant file.
"""

from lxml import etree, objectify
import os
import re
import xml.dom.minidom
import argparse

parser = argparse.ArgumentParser(description='')
parser.add_argument("input", help = 'input file')
parser.add_argument("output", help = 'output file name, end with xml')
args = vars(parser.parse_args())

result_dict = dict() #build the dict to hold the result

# the function that checks if a string is number of not
def check_number(text):
    outcome = True
    try:
        int_text = int(text)
    except:
        outcome = False

    return outcome

# the function to tokenize text, and put the tokenized text and its address, and its file to the dictionary
def tokenize_text(elem, address, file):
    #if the input word is empty, do nothing
    result = []
    if len(elem)!= 0:
        listelem = elem.split(' ') #first, split the input text by space
        reg = "[^0-9A-Za-z\u4e00-\u9fa5]"
        for item in listelem:
            if item != '': # check if the item is empty
                tokenized = re.sub(reg,'',item)
                if check_number(tokenized) == True:
                    #if the string end with , or . get rid of them
                    if item[-1] == ',' or item[-1] == '.':
                        result.append(item[:-1])
                    else:#otherwise, append them directly
                        result.append(item)
                else:
                    for element in re.sub(reg, ' ', item).split():
                        result.append(element)

    #format the address (where part)
    add_str = address[0]
    for i in range (1, len(address)):
        add_str += '.' + address[i]
    #for the result dictionary, add the filename before the address, and separate them by comma
    add_str = file + ',' + add_str

    #if the result list is empty, do nothing
    #otherwise, append the result, as well as the address, to the result dictionary
    if len(result) != 0:
        for item in result:
            if item in result_dict.keys():
                append = True
                for i in result_dict[item]:
                    if add_str != i:
                        continue
                    else:
                        append = False
                if append == True:
                    result_dict[item].append(add_str)
            else:
                result_dict[item] = [add_str]

#the function that deal with the attributes
def tokenize_attr(dict, address, file):
    #if the input dictionary is empty, do nothing
    if len(dict)!= 0:
        for key in dict:
            address.append('@'+key) #add @ to the attribute, to distinguish the attribute from element
            tokenize_text(dict[key], address, file)

#the function that simply strip the word
def strip(word):
    #word = word.replace(' ','')
    word = word.replace('\n','')
    word = word.replace('\t','')
    return word

#the function that find the path of the element (from the element to the root)
def getpath(item):
    message = ' '
    pathList = []
    #continue finding the parent element as long as there is no exception message
    while message != '\'NoneType\' object has no attribute \'tag\'':
        try:
            parent = item.getparent()
            pathList.append(str(parent.tag))
            item = parent
        except Exception as e:
            message = str(e)

    return pathList

#the list of files of input
fileList = os.listdir(args["input"])

for file in fileList:
    if file[-1] == 'l': #get rid of .DS_store
        f = open(args["input"]+'//'+file)
        parser = etree.XMLParser(remove_comments=True) #remove all of the comments
        tree = objectify.parse(f, parser=parser)
        root = tree.getroot()

        #start looping with the root
        curr = [root]
        next = []
        while curr: #continue looping as long as the current layer is not empty
            for item in curr:
                if item.text is not None: #in case that there is no value in the element
                    pathReverse = getpath(item) #get the path of the current element
                    pathList = pathReverse[::-1] #reverse the path list (so that it's from root to the element)
                    pathList.append(item.tag)
                    tokenize_text(strip(item.text), pathList,file) #call the two functions
                    tokenize_attr(item.attrib, pathList,file)
                    #print(pathList)
                    for child in item: #fill in the next list with the next layer
                        next.append(child)
            curr = next #update the two lists
            next = []

#for key in result_dict:
    #print(key+ ":",len(result_dict[key]))

#write index.xml file
doc = xml.dom.minidom.Document()
root = doc.createElement('index') #create root <index>
doc.appendChild(root) #add the root to the document

for key in result_dict:
    nodeToken = doc.createElement('token')
    nodeValue = doc.createElement('value')
    nodeValue.appendChild(doc.createTextNode(key))
    nodeToken.appendChild(nodeValue)
    for i in result_dict[key]:
        proList = i.split(',')
        nodeProvenance = doc.createElement('provenance')
        nodeWhich = doc.createElement('which')
        nodeWhich.appendChild(doc.createTextNode(proList[0]))
        nodeWhere = doc.createElement('where')
        nodeWhere.appendChild(doc.createTextNode(proList[1]))

        nodeProvenance.appendChild(nodeWhich)
        nodeProvenance.appendChild(nodeWhere)
        nodeToken.appendChild(nodeProvenance)

    root.appendChild(nodeToken)

fp = open(args["output"], 'w')
doc.writexml(fp, addindent='\t', newl='\n', encoding="utf-8")
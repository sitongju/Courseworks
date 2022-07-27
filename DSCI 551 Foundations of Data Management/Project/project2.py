"""
Sitong Ju
DSCI 551 Spring 2021
Project_part2
sitongju@usc.edu

the main part of the project
- tkinter interface that allow user to choose the dimensions and dates they want to query
- query the mysql database using pyspark(parallel processing)
- show the query result in the interface
- draw the query graph using matplotlib and show the graph in the interface
"""

import mysql.connector
from tkinter import *
from tkinter.ttk import *
from PIL import ImageTk,Image
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
import findspark
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.pyplot as plt
import pandas as pd


#setup mysql
#for this project, mysql is just used to get the list of country, location and continent for the UI
conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'project')
cur = conn.cursor()

#create empty lists to hold the items
#these lists of items will be put in the UI combobox to let the user to choose from
country_list = []
continent_list = []
location_list = []

cur.execute('select distinct(country_name) from country where country_name is not null')
for i in cur:
    if i[0] != '':
        country_list.append(i[0])

country_list = ['All'] + country_list

cur.execute('select distinct(continent) from country where continent is not null')
for i in cur:
    if i[0] != '':
        continent_list.append(i[0])
continent_list = ['All'] + continent_list

cur.execute('select distinct(location) from country where location is not null')
for i in cur:
    if i[0] != '':
        location_list.append(i[0])

location_list = ['All'] + location_list


#set up spark
findspark.add_packages('mysql:mysql-connector-java:8.0.24')

spark = SparkSession. \
    Builder(). \
    appName('sql'). \
    master('local'). \
    getOrCreate()

prop = {'user': 'dsci551',
        'password': 'Dsci-551',
        'driver': 'com.mysql.cj.jdbc.Driver'}
url = 'jdbc:mysql://localhost:3306/project'

#using spark to read mysql database
countrydata = spark.read.jdbc(url=url, table='country', properties=prop)
historydata = spark.read.jdbc(url=url, table='history', properties=prop)

#function to get the user choice in the top three boxes, and show the info in the info box
def returninfo():
    #get the user choices
    country = c1.get()
    location = c2.get()
    continent = c3.get()
    result = ''
    #three dimensions are independent.
    #show country info if country has an input, otherwise check if location has an input, and show location info
    #if location does not has an input, check continent
    ##meaning that if the user choose several dimensions at the same time, show the left one
    if country != 'All' and country != '':
        #using rdd parallel processing to find the country

        result = countrydata.rdd.repartition(10).filter(lambda r: r['country_name'] == country).collect()

        #calculate the number of current patients
        current = result[0]['confirmed_total'] - result[0]['recovered_total']-result[0]['deaths_total']
        # format the printing result
        printresult = 'Country name: '+result[0]['country_name']+'\n'\
                    +'Abbreviation: '+result[0]['abbreviation'] +'\n'\
                    +'Capital city: '+result[0]['capital_city'] +'\n'\
                    +'Iso: '+str(result[0]['iso'])+'\n'\
                    +'Location: '+result[0]['location']+'\n'\
                    +'Continent: '+result[0]['continent']+'\n'\
                    +'Area(sq_km): '+str(result[0]['sq_km_area'])+'\n'\
                    +'Population: '+str(result[0]['population']) + '\n'\
                    +'Life_expectancy: ' + str(result[0]['life_expectancy']) + '\n' \
                    +'Confirmed total: ' + str(result[0]['confirmed_total']) + '\n' \
                    +'Recovered total: ' + str(result[0]['recovered_total']) + '\n' \
                    +'Deaths total: ' + str(result[0]['deaths_total']) + '\n' \
                    +'Current patients: '+str(current)

    elif location != 'All' and location != '':
        #first calculate the aggregated number of each location
        locationdata = countrydata.groupBy('location').agg(fc.sum("confirmed_total").alias("confirmed_total"),
                                                           fc.sum("recovered_total").alias("recovered_total"),
                                                           fc.sum("deaths_total").alias("deaths_total")).select(
            "location", "confirmed_total", "recovered_total", "deaths_total")
        #then use rdd to filter the location we want to print
        result = locationdata.rdd.repartition(10).filter(lambda r: r['location'] == location).collect()
        #calculate the number of current patients and format the printing results
        current = result[0]['confirmed_total'] - result[0]['recovered_total'] - result[0]['deaths_total']
        printresult = 'Location: '+result[0]['location']+'\n'\
                    +'Confirmed total: ' + str(result[0]['confirmed_total']) + '\n' \
                    +'Recovered total: ' + str(result[0]['recovered_total']) + '\n' \
                    +'Deaths total: ' + str(result[0]['deaths_total']) + '\n' \
                    +'Current patients: '+str(current)

    elif continent != 'All' and continent != '':
        # first calculate the aggregated number of each continent
        continentdata = countrydata.groupBy('continent').agg(fc.sum("confirmed_total").alias("confirmed_total"),
                                                             fc.sum("recovered_total").alias("recovered_total"),
                                                             fc.sum("deaths_total").alias("deaths_total")).select(
            "continent", "confirmed_total", "recovered_total", "deaths_total")
        # then use rdd to filter the location we want to print
        result = continentdata.rdd.repartition(10).filter(lambda r: r['continent'] == continent).collect()
        # calculate the number of current patients and format the printing results
        current = result[0]['confirmed_total'] - result[0]['recovered_total'] - result[0]['deaths_total']
        printresult = 'Continent: ' + result[0]['continent'] + '\n' \
                      + 'Confirmed total: ' + str(result[0]['confirmed_total']) + '\n' \
                      + 'Recovered total: ' + str(result[0]['recovered_total']) + '\n' \
                      + 'Deaths total: ' + str(result[0]['deaths_total']) + '\n' \
                      + 'Current patients: ' + str(current)
    else:
        result = ''
        printresult = 'Please enter the right country name.'

    #put the result in the info label
    result1.config(text=printresult)

#a silly helper function that decides (for the returnplot function) which dimension and item to print out
def helper(country, location, continent):
    helperlist = []
    if country != 'All' and country != '':
        helperlist = ['country', country]
    elif location != 'All' and location != '':
        helperlist =['location',location]
    elif continent != 'All' and continent != '':
        helperlist = ['continent',continent]
    else:
        helperlist = []
    return helperlist

#function to get the user choice and dates, and and show the graph in the graph box
def returnplot():
    result = ''
    #get the user choices and input
    country = c1.get()
    location = c2.get()
    continent = c3.get()
    date11 = datebox1.get()
    date22 = datebox2.get()

    #call helper function to find which dimension to look for
    where = helper(country,location,continent) #this is a two-item list
    wherecountry = ''
    wherelocation = ''
    wherecontinent = ''
    if len(where) == 0:
        #this part has some problems. the info will be printed in the result2 label, but will be covered by the graph
        #so it will not show such infomation, but an empty graph
        #needs to be fixed: create a new label to put error info
        result = 'Please select one place(country or location or continent) to see the plot'
        result2.config(text=result)
    elif date11 > date22 or date11 < '2020-01-22' or date22 > '2021-04-23':
        #this too
        result = 'Please make sure the date interval is valid'
        result2.config(text=result)
    elif where[0] == 'country':
        wherecountry = where[1]
        result = historydata.rdd.repartition(10).filter(
            lambda r: r['dates'] >= date11 and r['dates'] <= date22 and r['country_name'] == wherecountry).sortBy(lambda r: r['dates'],ascending=True).collect()
    elif where[0] == 'location':
        wherelocation = where[1]
        #first find all the countries in the location in the country table
        filtercountry = countrydata[countrydata.location == wherelocation][['country_name']]
        #join history table and country table, find the history data given the dates
        finddata = historydata[(historydata.dates >= date11)&(historydata.dates <= date22)].join(filtercountry,historydata.country_name == filtercountry.country_name).select(historydata['country_name'],\
                                                        historydata['dates'], historydata['confirmed'], historydata['recovered'], historydata['deaths'])
        #aggregate the numbers by dates
        agg = finddata.groupBy('dates').agg(fc.sum("confirmed").alias("confirmed"),fc.sum("recovered").alias("recovered"),
                                                       fc.sum("deaths").alias("deaths")).select("dates", "confirmed","recovered","deaths")
        #sort the result using rdd
        result = agg.rdd.repartition(10).sortBy(lambda r: r['dates'],ascending=True).collect()
    elif where[0] == 'continent':
        wherecontinent = where[1]
        filtercontinent = countrydata[countrydata.continent == wherecontinent][['country_name']]
        finddata = historydata[(historydata.dates >= date11) & (historydata.dates <= date22)].join(filtercontinent,
                                                                                                 historydata.country_name == filtercontinent.country_name).select(
            historydata['country_name'], historydata['dates'], historydata['confirmed'], historydata['recovered'], historydata['deaths'])
        agg = finddata.groupBy('dates').agg(fc.sum("confirmed").alias("confirmed"),
                                            fc.sum("recovered").alias("recovered"),
                                            fc.sum("deaths").alias("deaths")).select("dates", "confirmed", "recovered", "deaths")
        result = agg.rdd.repartition(10).sortBy(lambda r: r['dates'],ascending=True).collect()

    #the lists used to transfer the result to pandas dataframe
    result_dates = []
    result_confirmed = []
    result_recovered = []
    result_deaths = []
    result_current = []

    if isinstance(result, list) :
        for i in result:
            #fill in the lists with the items in the result
            result_dates.append(i['dates'])
            result_confirmed.append(i['confirmed'])
            result_recovered.append(i['recovered'])
            result_deaths.append(i['deaths'])
            result_current.append(i['confirmed']-i['recovered']-i['deaths'])
    else:
        #this part has same problem as the one previously mentioned
        result = 'Please make sure the data entered is valid'
        result2.config(text=result)

    #build pandas dataframe (in order to draw plot)
    data_trans = {"dates":result_dates,"confirmed":result_confirmed,"recovered":result_recovered,"deaths":result_deaths,"current":result_current}
    df = pd.DataFrame(data_trans)

    #draw plot using matplotlib

    df["dates"] = pd.to_datetime(df["dates"], format='%Y-%m-%d')
    x = df.dates
    y1 = df['confirmed']
    y2 = df['recovered']
    y3 = df['deaths']
    y4 = df['current']

    fig = plt.figure(figsize=(7, 4))
    ax = fig.add_subplot(111)
    ax.plot(x, y1, label="confirmed")
    ax.plot(x, y2, label="recovered")
    ax.plot(x, y3, label="deaths")
    ax.plot(x, y4, label="current")

    plt.xlabel("date", fontsize=8)
    plt.legend(loc=1, fontsize=8)
    plt.xticks(fontsize=7)
    plt.yticks(fontsize=7)
    plt.title('Covid-19 stats of '+ where[1]+ ' over time', fontsize=10)

    #put the plot to the canvas
    canvas = FigureCanvasTkAgg(fig, master=root)
    canvas.draw()
    canvas.get_tk_widget().place(relx=0.38, rely=0.48, height=310, width=600)

    #add the toolbar of the plot to the tkinter interface
    #toolbar = NavigationToolbar2Tk(canvas, root)
    #toolbar.update()
    #canvas._tkcanvas.place(relx = 0.38,rely = 0.48,height=310, width=600)

#the function that bind with ok button
def calc3():
    returninfo()
    returnplot()

#setup tkinter and build the interface
root = Tk()
root.title('Covid-19 Statistics Query')
root.geometry('1200x800')

var1 = StringVar()
var2 = StringVar()
var3 = StringVar()
var4 = StringVar()
var5 = StringVar()

image2 =Image.open('bg.png')
background_image = ImageTk.PhotoImage(image2)
background_label = Label(image=background_image)
background_label.place(x=0, y=0, relwidth=1, relheight=1)

#add title
title = Label(root,text = 'Covid-19 Data Query', font = ('Arial',25))
title.place(relx=0.35,rely=0.05)

#add prompt for the user
info1 = Label(root,text = 'Country data: (Choose one option, or choose \'All\'. Please just choose one dimension at a time)')
info1.place(relx=0.04,rely=0.12)

#add the country label and the country box
country = Label(root,text = 'Country')
country.place(relx=0.04, rely=0.17)
c1 = Combobox(root, textvariable=var1,state='normal', values=country_list)
c1.place(relx=0.04, rely=0.22, height=30,width=200)

#add the location label and the location box
location = Label(root,text = 'Location')
location.place(relx = 0.28, rely=0.17)
c2 = Combobox(root, textvariable=var2, values=location_list)
c2.place(relx=0.28, rely=0.22, height=30,width=200)

#add the contiennt label and the continent box
continent = Label(root,text = 'Continent')
continent.place(relx = 0.52, rely=0.17)
c3 = Combobox(root, textvariable=var3, values=continent_list)
c3.place(relx=0.52, rely=0.22, height=30,width=200)

#add prompt for the user to enter dates
info2 = Label(root,text = 'Graph of Covid-19 data by date: (Date available: 2020-01-22 to 2021-04-23)')
info2.place(relx=0.04, rely = 0.30)
date1 = Label(root,text = 'From: (yyyy-mm-dd)')
date1.place(relx = 0.04, rely=0.35)
datebox1=Entry(root)
datebox1.place(relx=0.04, rely=0.40, height=30,width=200)

date2 = Label(root,text = 'To: (yyyy-mm-dd)')
date2.place(relx = 0.28, rely=0.35)
datebox2=Entry(root)
datebox2.place(relx=0.28, rely=0.40, height=30,width=200)

#add ok button. bind the calc3 function (which will call the returninfo and returnplot functions) with the button
ok = Button(root,text = 'ok',command = calc3)
ok.place(relx = 0.52,rely=0.40,height = 30, width = 120)

#add the label to put the result
result1 = Label(root, text='Info',anchor = CENTER)
result1.place(relx=0.04, rely=0.48, height=310,width=350 )

result2 = Label(root, text = 'Graph',anchor = CENTER)
result2.place(relx=0.38, rely=0.48,height=310,width=600)

root.mainloop()


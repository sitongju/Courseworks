"""
Sitong Ju
DSCI 551 Spring 2021
Project_part1
sitongju@usc.edu

read in the data through API and store them in mysql database

"""
import mysql.connector
import requests

conn = mysql.connector.connect(user='dsci551', password='Dsci-551', host='localhost',database = 'project')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS history;')
cur.execute('DROP TABLE IF EXISTS country;')

create_country_sql = """
CREATE TABLE country(
    country_name varchar(50) PRIMARY KEY,
    abbreviation varchar(5),
    confirmed_total int,
    recovered_total int,
    deaths_total int,
    population bigint,
    sq_km_area int,
    life_expectancy real,
    continent varchar(20),
    location varchar(100),
    iso int,
    capital_city varchar(50)

);
"""
cur.execute(create_country_sql)

create_history_sql = """
CREATE TABLE history(
    country_name varchar(50),
    dates varchar(15),
    confirmed int,
    recovered int,
    deaths int,
    PRIMARY KEY (country_name, dates),
    FOREIGN KEY (country_name) REFERENCES country (country_name)
);
"""

cur.execute(create_history_sql)

url1 = 'https://covid-api.mmediagroup.fr/v1/cases'
url2 = 'https://covid-api.mmediagroup.fr/v1/history?status=confirmed'
url3 = 'https://covid-api.mmediagroup.fr/v1/history?status=recovered'
url4 = 'https://covid-api.mmediagroup.fr/v1/history?status=deaths'

query_response = requests.get(url1)
countries = query_response.json()


for i in countries:
    country_name = i
    abbreviation = countries[i]['All'].get('abbreviation','')
    confirmed_total = countries[i]['All']['confirmed']
    recovered_total = countries[i]['All']['recovered']
    deaths_total = countries[i]['All']['deaths']
    population = countries[i]['All'].get('population', '-1')
    sq_km_area = countries[i]['All'].get('sq_km_area', '-1')
    life_expectancy = countries[i]['All'].get('life_expectancy', '-1')
    continent = countries[i]['All'].get('continent', '')
    location = countries[i]['All'].get('location', '')
    iso = countries[i]['All'].get('iso', '-1')
    capital_city = countries[i]['All'].get('capital_city', '')


    cur.execute('INSERT INTO country(country_name,abbreviation,confirmed_total,recovered_total,deaths_total,'
                'population,sq_km_area,life_expectancy,continent,location,iso,capital_city) '
                'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(country_name,abbreviation,confirmed_total,recovered_total,
                deaths_total,population,sq_km_area,life_expectancy,continent,location,iso,capital_city,))

#cur.execute('select * from country')
#print(cur)

confirmed_result = requests.get(url2)
recovered_result = requests.get(url3)
deaths_result = requests.get(url4)
confirmed = confirmed_result.json()
recovered = recovered_result.json()
deaths = deaths_result.json()


for i in confirmed:
    country_name = i
    confirmed_dates = confirmed[i]['All']['dates']
    recovered_dates = recovered[i]['All']['dates']
    deaths_dates = deaths[i]['All']['dates']
    for date in confirmed_dates:
        cur.execute('INSERT INTO history(country_name,dates,confirmed,recovered,deaths) VALUES (%s,%s,%s,%s,%s)',
                    (country_name,date,confirmed_dates[date],recovered_dates[date],deaths_dates[date],))


conn.commit()
cur.close()
conn.close()
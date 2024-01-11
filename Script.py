import requests
import json
from datetime import datetime
import sqlalchemy
import pyodbc
import pandas as pd

url = "https://zillow56.p.rapidapi.com/search"

querystring = {"location":"houston, tx"}

headers = {
	"X-RapidAPI-Key": "##API KEY",
	"X-RapidAPI-Host": "##API HOST"
}

response = requests.get(url, headers=headers, params=querystring)
response_data = response.json()


df =pd.json_normalize(response_data , 'results')

required_cols = ['bathrooms',
'bedrooms',
'city',
'country',
'homeStatus',
'homeType',
'imgSrc',
'isFeatured',
'latitude',
'livingArea',
'longitude',
'lotAreaUnit',
'lotAreaValue',
'price',
'state',
'streetAddress',
'zipcode',
]


df_final = df[required_cols]

df_final.to_csv('test.csv')


engine=sqlalchemy.create_engine('mssql+pyodbc://./Zillow?driver=SQL+Server+Native+Client+11.0')
df_final.to_sql(name= 'Fact_zillow' , con=engine, index=False, if_exists='replace')

print(df_final)

import pandas as pd
import requests
import json
import csv
import os
from datetime import date

"""
print("test mi")
url = 'https://api.openweathermap.org/data/2.5/weather?q=London&appid=c31b6e2d6116221237f8f644e3c95605'
print(url)

res = requests.get(url)
json_it = res.json()

df = pd.json_normalize(json_it)

#writing data in csv format
df.to_csv("test.csv")
"""

# get today date
today_is = date.today().strftime("%Y-%m-%d")
#print(today_is) #output: 2023-03-14 #test code

# make a directory to save the downloaded csv files
save_dir = "/app/data"

# to create the directory if not existed
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

country = ["australia", "singapore", "malaysia"]

for i in range(len(country)):
    # Set up the API endpoint URL
    url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/" + country[i] + "/" + today_is + "/" + today_is + "?unitGroup=metric&include=days&key=8MJQ4966D6CWWU5GBV7KBHBDS&contentType=csv"
    #print(url) #test code
    df = pd.read_csv(url)

    # set up the path to save the csv files
    filePath = os.path.join(save_dir, "data_"+country[i]+".csv")

    # save the csv files
    df.to_csv(filePath, index=False)

    #df.to_csv("data_"+country[i]+".csv")
    #print("data_"+country[i]+".csv") #test code

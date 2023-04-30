import numpy as np
import pandas as pd
import requests
import time
from datetime import date
import json
import os


# set WeatherAPI key and locations
api_key = '029a436baa02494fbe583505230304'
locations = ['Singapore', 'Australia', 'Malaysia'] #this have to be change to accomodate user define

# get today date
Date_is = date.today().strftime("%Y-%m-%d") # this code have to be change so that user can define the date to get
#print(today_is) #test code

# make an empty dataframe
df_all=[]

# make a directory to save the downloaded csv files
save_dir = "/app/data"
# to create the directory if not existed
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

def main():
    for location in locations:
        try:
            # api call - data receive in json
            url = f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={Date_is}"
            response = requests.get(url)

            #check for any error
            response.raise_for_status()

            data = json.loads(response.text)

            # Create a DataFrame from the 'hour' list in the JSON data
            df = pd.DataFrame(data["forecast"]["forecastday"][0]["hour"])

            # Add new columns for location and time information
            df["location"] = location
            df["time"] = df["time"].str.split("+").str[0]

            # Parse the 'time' column to extract the hour information
            df["hour"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M").dt.hour

            # Set the 'time' and 'location' columns as the index
            df.set_index(["location", "time"], inplace=True)

            # Append the DataFrame to the list
            df_all.append(df)

        except Exception as e:
            print("Error occur: ",e)

    # Concatenate the DataFrames into a single DataFrame
    concated_df = pd.concat(df_all)

    # set up the path to save the csv files
    filePath = os.path.join(save_dir, "concated_df.csv")

    # Save the DataFrame to a CSV file
    concated_df.to_csv(filePath, index=True)

if __name__ == "__main__":
    main()

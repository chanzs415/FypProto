import numpy as np
import pandas as pd
import requests
import time

# set WeatherAPI key and locations
api_key = '029a436baa02494fbe583505230304'
locations = ['Singapore', 'Australia', 'Malaysia']

def main():
    #weather_df = pd.DataFrame(columns=['Location', 'Last Updated', 'Temperature (C)', 'Temperature (F)', 'Wind (km/hr)',          'Wind direction (in degree)', 'Wind direction (compass)', 'Pressure (millibars)',           'Precipitation (mm)', 'Humidity', 'Cloud Cover', 'UV Index', 'Wind gust (km/hr)'])
    #weather_df.to_csv('data.csv', mode='a', header=True, index=False) #append data to csv file      
    while True:
        for loc in locations:
            # create empty lists to store weather data
            last_updated = []
            temp_c = []
            precip_mm = []
            humidity = []
            cloud = []
            uv = []
            temp_f = []
            wind_kph = []
            wind_degree = []
            wind_dir =[]
            pressure_mb = []
            gust_kph = []

            try:
                url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={loc}&field=last_updated,temp_c,temp_f,wind_kph,wind_degree,wind_dir,pressure_mb,precip_mm,humidity,cloud,uv,gust_kph"
                response = requests.get(url)
                response.raise_for_status()  # raise an exception for any errors
                data = response.json()
                last_updated.append(data['current']['last_updated'])
                temp_c.append(data['current']['temp_c'])
                temp_f.append(data['current']['temp_f'])
                wind_kph.append(data['current']['wind_kph'])
                wind_degree.append(data['current']['wind_degree'])
                wind_dir.append(data['current']['wind_dir'])
                pressure_mb.append(data['current']['pressure_mb'])
                precip_mm.append(data['current']['precip_mm'])
                humidity.append(data['current']['humidity'])
                cloud.append(data['current']['cloud'])
                uv.append(data['current']['uv'])
                gust_kph.append(data['current']['gust_kph'])

                # create dataframe to store weather data
                weather_df = pd.DataFrame({
                    'Location': [loc],
                    'Last Updated': last_updated,
                    'Temperature (C)': temp_c,
                    'Temperature (F)': temp_f,
                    'Wind (km/hr)': wind_kph,
                    'Wind direction (in degree)': wind_degree,
                    'Wind direction (compass)': wind_dir,
                    'Pressure (millibars)': pressure_mb,
                    'Precipitation (mm)': precip_mm,
                    'Humidity': humidity,
                    'Cloud Cover': cloud,
                    'UV Index': uv,
                    'Wind gust (km/hr)' : gust_kph
                })
                # store data in a CSV file
                weather_df.to_csv('data.csv', mode='a', header=False, index=False) #append data to csv file
                # print the dataframe
                #print(weather_df)
                
            except Exception as e:
                print("Error occurred: ", e)

            # wait for 60 seconds before sending the next batch of data
            time.sleep(60)

if __name__ == "__main__":
    main()



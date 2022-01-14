import pandas as pd
import requests

def historical_weather(city, state, start_date, end_date, API_key):
    """
    Returns a JSON call containing historical daily weather data from a given city for a date range no greater than 250 days.
    """
   
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history?&aggregateHours=24&startDateTime={start_date}T00:00:00&endDateTime={end_date}T00:00:00&unitGroup=metric&contentType=json&dayStartTime=0:0:00&dayEndTime=0:0:00&location={city},{state},US&key={API_key}"
    
    return requests.request('GET', url)


def build_weather_table(locations: list, year: int, output_doc_name: str, API_key: str):
    """
    Builds a weather table and saves it as a csv file. Does not return any value.
    """
    
    dates = {
        'start': [f'{year}-01-01', f'{year}-09-07'],
        'end': [f'{year}-09-06', f'{year}-12-31']
    }

    for location in locations:
        
        weather_data = [None, None]
        
        for i in range(2):
            response = historical_weather(city=location['city'], state=location['state'], start_date=dates['start'][i], end_date=dates['end'][i], API_key=API_key)
            weather_data[i] = response.json()['locations'][f'{location["city"]},{location["state"]},US']['values']

        df = pd.concat([pd.DataFrame(weather_data[0]), pd.DataFrame(weather_data[1])])
        df['city'] = location['city']
        df['state'] = location['state']
        
        try:
            weather_table = pd.concat([weather_table, df])
        except NameError:
            weather_table = df.copy()

    weather_table.to_csv(output_doc_name)


# if __name__ == '__main__':
    
#     API_key = '<YOUR API KEY GOES HERE>'
#     locations_file_path = '<THE LOCATIONS FILE PATH GOES HERE>'
#     year = <THE YEAR FOR THE DATA YOU WANT TO RECEIVE>

#     locations_df = pd.read_csv(locations_file_path)
#     locations_df = locations_df.drop('Unnamed: 0', axis =1)
#     locations_list = []
#     for i in range(locations_df.shape[0]):
#         locations_list.append({'city': locations_df.loc[i, 'city'], 'state': locations_df.loc[i, 'state']})

#     build_weather_table(locations=locations_list, year=year, output_doc_name=f'historical_daily_weather_data_{year}.csv', API_key=API_key)
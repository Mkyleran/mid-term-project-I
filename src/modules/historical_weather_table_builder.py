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
        
        response = [None, None]
        
        for i in range(2):
            response[i] = historical_weather(city=location['city'], state=location['state'], start_date=dates['start'][i], end_date=dates['end'][i], API_key=API_key)
        
        df = pd.concat([pd.DataFrame(response[0]), pd.DataFrame(response[1])])
        df['city'] = location['city']
        df['state'] = location['state']
        
        try:
            weather_table = pd.concat([weather_table, df])
        except NameError:
            weather_table = df.copy()

    weather_table.to_csv(output_doc_name)


if __name__ == '__main__':
    
    API_key = '<YOUR API KEY GOES HERE>'
    locations_file_path = '<THE LOCATIONS FILE PATH GOES HERE>'
    year = <THE YEAR FOR THE DATA YOU WANT TO RECEIVE>

    locations = pd.read_csv(locations_file_path).to_list()
    build_weather_table(locations=locations, year=year, output_doc_name=f'historical_daily_weather_data_{year}.csv', API_key=API_key)
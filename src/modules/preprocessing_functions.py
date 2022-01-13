import pandas as pd
import numpy as np

def daily_flight_order(df): 
    """
    Returns the pandas dataframe ordered by [fl_date, tail_num, crs_dep_time] with an added column indicating how many flights that plane has undertaken previously during the same day.   
    """
    
    data = df.sort_values(['fl_date', 'tail_num', 'crs_dep_time']).reset_index(drop = True)
    data['n_previous_flights'] = 0
    
    for table_index in range(1, data.shape[0]):
        if data.loc[table_index, 'tail_num'] != data.loc[table_index - 1, 'tail_num']:
            continue
        data.loc[table_index, 'n_previous_flights'] = data.loc[table_index - 1, 'n_previous_flights'] + 1
        
    return data


def flight_test_features(df, purged = False):
    """
    Returns a pandas DataFrame containing only the feature set that will be used to test the machine learning model.
    
    Parameters
    ----------
    df: pandas DataFrame
    
    purged: bool, default = False
        When set to True, the function will also call purge_features to remove those that were determined not to be desirable for the machine learning model.
    
    """
    
    features = [
        'fl_date',
        'mkt_unique_carrier',
        'branded_code_share',
        'mkt_carrier',
        'mkt_carrier_fl_num',
        'op_unique_carrier',
        'tail_num',
        'op_carrier_fl_num',
        'origin_airport_id',
        'origin',
        'origin_city_name',
        'dest_airport_id',
        'dest',
        'dest_city_name',
        'crs_dep_time',
        'crs_arr_time',
        'dup',
        'crs_elapsed_time',
        'flights',
        'distance'
    ]
    df = df[features]
    if purged:
        return purge_features(df)  
    return df


def purge_features(df):
    """
    Returns a pandas DataFrame having removed the features that were determined not to be desirable as part of the machine learning model.
    """

#     Complete feature set provided for the machine learning model are as follows: 
#     features = [
#         'fl_date',
#         'mkt_unique_carrier',
#         'branded_code_share',
#         'mkt_carrier',
#         'mkt_carrier_fl_num',
#         'op_unique_carrier',
#         'tail_num',
#         'op_carrier_fl_num',
#         'origin_airport_id',
#         'origin',
#         'origin_city_name',
#         'dest_airport_id',
#         'dest',
#         'dest_city_name',
#         'crs_dep_time',
#         'crs_arr_time',
#         'dup',
#         'crs_elapsed_time',
#         'flights',
#         'distance'
#     ]
    
    features_to_remove = [
        'mkt_unique_carrier',
        'branded_code_share',
        'mkt_carrier',
        'mkt_carrier_fl_num',
        'op_carrier_fl_num',
        'origin_airport_id',
        'dest_airport_id',
        'dup',
        'crs_elapsed_time',
        'flights'
    ]
    
    return df.drop(columns=features_to_remove, axis = 1)


def process_nan_values(df, features_to_zero = [], features_to_remove = [], features_to_mean = [], features_to_median = [], avg_before_purge = True):
    """
    Returns a pandas DataFrame with the NaN values replaced or removed.
    """
    
    df.reset_index(drop = True, inplace = True)
    
    for feature in features_to_zero:
        zero_column = df[feature].fillna(0)
        df[feature] = zero_column
    
    if avg_before_purge:
        for feature in features_to_mean:
            mean = df[feature].mean()
            mean_column = df[feature].fillna(mean)
            df[feature] = mean_column
        
        for feature in features_to_median:
            median = df[feature].median()
            median_column = df[feature].fillna(median)
            df[feature] = median_column
    
    df = df.iloc[df[features_to_remove].dropna().index]
    
    if not avg_before_purge:
        for feature in features_to_mean:
            mean = df[feature].mean()
            mean_column = df[feature].fillna(mean)
            df[feature] = mean_column
        
        for feature in features_to_median:
            median = df[feature].median()
            median_column = df[feature].fillna(median)
            df[feature] = median_column
    
    return df.reset_index(drop = True)


def datetime_binning(df, bin_set = {}):
    """
    Returns a pandas DataFrame with an additional column(s) of the flight dates binned by departure hour, day, weekday, week, and/or month of the year.
    
    Parameters
    ----------
    df: pandas DataFrame
    
    bin_set: iterable
        Must be any combination of:
            'h' = hour
            'd' = day
            'wd' = weekday
            'w' = week
            'm' = month
    """
    
    df.reset_index(drop = True, inplace = True)
    
    if not set(bin_set).issubset({'h', 'd', 'wd', 'w', 'm'}):
        raise ValueError("bin_set must be any of 'd', 'wd', 'w', or 'm'")
    
    fl_date = np.array(df['fl_date'])
    shape = df.shape[0]
    
    if 'h' in bin_set:
        df['dep_hour'] = df['crs_dep_time']//100
    if 'd' in bin_set:
        day_of_year = np.empty(shape = shape)
    if 'wd' in bin_set:
        weekday = np.empty(shape = shape)    
    if 'w' in bin_set:
        week = np.empty(shape = shape)
    if 'm' in bin_set:
        month = np.empty(shape = shape)
    
    for i in range(shape):
        try:
            date_code = pd.to_datetime(fl_date[i], utc=True, unit='ms')
        except ValueError:
            date_code = pd.to_datetime(fl_date[i])   
    
        if 'd' in bin_set:
            day[i] = date_code.day_of_year
            
        if 'wd' in bin_set:
            weekday[i] = date_code.day_of_week

        if 'w' in bin_set:
            week[i] = date_code.weekofyear

        if 'm' in bin_set:
            month[i] = date_code.month
    
    if 'd' in bin_set:
        df['day_of_year'] = day
    if 'wd' in bin_set:
        df['weekday'] = weekday
    if 'w' in bin_set:
        df['week'] = week
    if 'm' in bin_set:
        df['month'] = month
    
    return df


def is_stat_holiday(df):
    """
    Returns a pandas DataFrame with an additional column of the whether or not the flight is taking place on a holiday. 
    """
    
    holiday_days_list = [
        #New Years
        '2019-1-1',
        
        #MLK Jr Day
        '2019-1-18',
        '2019-1-19',
        '2019-1-20',
        '2019-1-21',
        
        #President's Day
        '2019-2-15',        
        '2019-2-16',
        '2019-2-17',
        '2019-2-18',
        
        #Memorial Day
        '2019-5-24',
        '2019-5-25',
        '2019-5-26',
        '2019-5-27',
        
        #Independence Day
        '2019-7-3',
        '2019-7-4',
        '2019-7-5',
        '2019-7-6',
        '2019-7-7',
        
        #Labor Day
        '2019-8-30',
        '2019-8-31',
        '2019-9-1',
        '2019-9-2',
        
        #Columbus Day
        '2019-10-11',
        '2019-10-12',
        '2019-10-13',
        '2019-10-14',
        
        #Veteran's Day
        '2019-11-8',
        '2019-11-9',
        '2019-11-10',
        '2019-11-11',
        
        #Thanksgiving
        '2019-11-25',
        '2019-11-26',
        '2019-11-27',
        '2019-11-28',
        
        #Christmas
        '2019-12-21',
        '2019-12-22',
        '2019-12-23',
        '2019-12-24',
        '2019-12-25',
        '2019-12-26',
        '2019-12-27',
        '2019-12-28',
        '2019-12-29',
        
        #New Years
        '2019-12-30',
        '2019-12-31',
        '2020-1-1'
    ]
    
    df.reset_index(drop = True, inplace = True)
    df['stat_holiday'] = 0
    
    for i in range(df.shape[0]):
        try:
            timestamp = pd.to_datetime(df.loc[i, 'fl_date'], utc=True, unit='ms')
            df.loc[i, 'stat_holiday'] = int(f"{timestamp.year}-{timestamp.month}-{timestamp.day}" in holiday_days_list)
        except ValueError:
            timestamp = pd.to_datetime(df.loc[i, 'fl_date'])
            df.loc[i, 'stat_holiday'] = int(f"{timestamp.year}-{timestamp.month}-{timestamp.day}" in holiday_days_list)
        
    return df


def numerical_categorical_split(df):
    """
    Returns two pandas DataFrames having segregated the two. First DataFrame is numerical and the second is categorical.
    """
    
    categorical_features_list = [
        'fl_date',
        'mkt_unique_carrier',
        'branded_code_share',
        'mkt_carrier',
        'mkt_carrier_fl_num',
        'op_unique_carrier',
        'tail_num',
        'op_carrier_fl_num',
        'origin_airport_id',
        'origin',
        'origin_city_name',
        'dest_airport_id',
        'dest',
        'dest_city_name',
        'dup',
        'flights'
    ]   
    
    df_cat_features_list = []
    for feature in df.columns:
        if feature in categorical_features_list:
            df_cat_features_list.append(feature)
    
    df_categorical = df[df_cat_features_list]
    
    df_numerical_features_list = list(df.columns)
    for cat_feature in df_cat_features_list:
        df_numerical_features_list.remove(cat_feature)
    
    df_numerical = df[df_numerical_features_list]
    
    return df_numerical, df_categorical


# Function for getting information binomial probabilities at a certain threshold. Can be used in Data cleaning to make judgements about what data to eliminate (e.g. See the percentages, by carrier, of flights with delays over 120 mins)

def binomial_stats(df, col_list, threshold=0, greater=True):
    '''Returns the bionomial distribution of a categorical feature that can be aggregated and the desired frequency proportions.
        Parameters:
            a (Pandas Data Frame) df - Date frame.
            b (float or int) threshold (default = 0) - Numeric value to determine true or false cut-off threshold.
            c (boolean) greater (default = True) - Boolean to determine if the comparison operator is greater than or less than.
        Returns:
            New Pandas Data Frame
    '''
    if len(col_list) != 2:
            raise Exception("'Error. The columns list must only contain two column names: grouping feature and frequency feature.")
            
    stats = df[col_list]
    group = col_list[0]
    col_filt = col_list[1]
    col_1 = col_filt + "_" + "yes"
    col_2 = col_filt + "_" + "no"
    if greater == True:

        filt = stats.apply(lambda x : True
            if x[col_filt] > threshold else False, axis = 1)
    else:
        filt = stats.apply(lambda x : True
            if x[col_filt] < threshold else False, axis = 1)
        

    yes = stats[filt].groupby(group).count()
    total = stats.groupby(group).count()
    stats = yes/total     
    stats[col_2] = (1- yes/total)
    stats.rename({col_filt: col_1}, axis=1, inplace=True)
    
    return stats.reset_index()

# Version 2 Doesn't have weekday option
def datetime_binning_v2(df, bin_set = {}):
    """
    Returns a pandas DataFrame with an additional column(s) of the flight dates binned by departure hour, day, weekday, week, and/or month of the year.
    
    Parameters
    ----------
    df: pandas DataFrame
    
    bin_set: iterable
        Must be any combination of:
            'h' = hour
            'd' = day of month
            'wd' = weekday -> NOT AVAILABLE 
            'w' = week
            'm' = month
    """
    
    df.reset_index(drop = True, inplace = True)
    
    if not set(bin_set).issubset({'h', 'd', 'w', 'm', 'wd'}):
        raise ValueError("bin_set must be any of 'wd', 'h', 'd', 'w', or 'm'")
        
    if 'h' in bin_set:
        df['dep_hour'] = df['crs_dep_time']//100       
    
    if 'd' in bin_set:
        df['day_of_month'] = 0
        for i in range(df.shape[0]):
            try:
                df.loc[i, 'day_of_month'] = pd.to_datetime(df.loc[i, 'fl_date'], utc=True, unit='ms').day
            except ValueError:
                df.loc[i, 'day_of_month'] = pd.to_datetime(df.loc[i, 'fl_date']).day
                
#     if 'wd' in bin_set:
#         df['weekday'] = 0
#         for i in range(df.shape[0]):
#             try:
#                 df.loc[i, 'weekday'] = pd.to_datetime(df.loc[i, 'fl_date'], utc=True, unit='ms').weekday
#             except ValueError:
#                 df.loc[i, 'weekday'] = pd.to_datetime(df.loc[i, 'fl_date']).weekday
    
    if 'w' in bin_set:
        df['week_of_year'] = 0
        for i in range(df.shape[0]):
            try:
                df.loc[i, 'week_of_year'] = pd.to_datetime(df.loc[i, 'fl_date'], utc=True, unit='ms').week
            except ValueError:
                df.loc[i, 'week_of_year'] = pd.to_datetime(df.loc[i, 'fl_date']).week
    
    if 'm' in bin_set:
        df['month'] = 0
        for i in range(df.shape[0]):
            try:
                df.loc[i, 'month'] = pd.to_datetime(df.loc[i, 'fl_date'], utc=True, unit='ms').month
            except ValueError:
                df.loc[i, 'month'] = pd.to_datetime(df.loc[i, 'fl_date']).month
        
    return df

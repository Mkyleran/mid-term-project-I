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
    
    if purged:
        return purge_features(df)  
    return df[features]


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
    
    return df.drop(columns_to_remove, axis = 1)


def process_nan_values(df, features_to_zero = [], features_to_remove = [], features_to_mean = [], features_to_median = [], avg_before_purge = True):
    """
    Returns a pandas DataFrame with the NaN values replaced or removed.
    """
   
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
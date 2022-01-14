import pandas as pd

# Project level modules
import modules.preprocessing_functions as ppf
import modules.save_model as sm

def load_and_process(csv_path: 'str', time_period: 'str'):
    """
    Load the csv, process NAN values in the target variable, and
    drop irrelevant rows
    
    Parameters
    ----------
    csv_path : string
    time_period : string 'week', 'month'
    
    Returns
    -------
    df : Pandas Dataframe
    """
    
    # Load csv and parse the first column as dates
    df = pd.read_csv(csv_path, parse_dates=[0])
    
    # Filter time period
    if time_period == 'week':
        df = df[((df['fl_date'] >= f'2018-01-01') &
                (df['fl_date'] <= f'2018-01-07')) |
                ((df['fl_date'] >= f'2019-01-01') &
                 (df['fl_date'] <= f'2019-01-07')) |
                ((df['fl_date'] >= f'2020-01-01') &
                 (df['fl_date'] <= f'2020-01-07'))
               ]
    
    # Set NAN values in departure and arrival delay to 0
    df = ppf.process_nan_values(
        df=df,
        features_to_zero=['dep_delay', 'arr_delay']
    )
    
    # Drop flight rows that were cancelled or diverted
    df = df[
        (df['cancelled'] == 0) &
        (df['diverted'] == 0)
    ]
    
    # Drop flights with delay >+3std and <-120min
    df = df[
        (df['arr_delay'] < (df['arr_delay'].mean()
                            + 3 * df['arr_delay'].std())) &
        (df['arr_delay'] > -120)
    ]
    
    # Add stratifier
    df['is_delayed'] = 0
    df.loc[(df['arr_delay'] > 0), 'is_delayed'] = 1
    
    return df


def week_month(df, time_period: 'str' = 'week'):
    """
    
    Parameters
    ----------
    df : Pandas DataFrame
    time_period : string 'week', 'month'
        
    Returns
    -------
    df : Pandas DataFrame
    """
    
    feature_dict = {
        'origin' : 'dep_delay',
        'origin_city_name' : 'dep_delay',
        'dest' : 'arr_delay',
        'dest_city_name' : 'arr_delay',
        'tail_num' : 'arr_delay',
        'op_unique_carrier' : 'arr_delay'
    }
    
    for k, v in feature_dict.items():
        stats = pd.read_csv(
            f'../data/feature_average_delay_stats/{k}_{v}_stats.csv',
            index_col=[0]
        )
        
        df[f'{k}_{time_period}_mean_{v}'] = (
            df[k].map(stats[f'2018_{time_period}_{k}_mean_{v}'])
        )
    
        df.drop(k, axis=1, inplace=True)
    
    return df


def load(data_set: 'str' = 'sample', time_period: 'str' = 'week'):
    """
    
    Parameters
    ----------
    data_set : string 'full', 'sample'
        'full' is the whole 2019 csv with 638,649 lines
        'sample' is 10,000 lines randomly sampled from 2018-01.csv
    time_period : string 'week', 'month'
    
    Returns
    -------
    data : Pandas DataFrame
    """
    
    google_drive_path = ('~/Google Drive/My Drive/Lighthouse Labs/'
                         + 'Mid-term Project/Data-Jan/')
    
    path = {
        'full' : f'{google_drive_path}2019-01.csv',
        'sample' : f'../data/sample.csv'
    }
    
    # Load the first week of to predict for
    data = load_and_process(csv_path=path[data_set], time_period='week')
    
    # Convert date to day integer
    data['fl_date'] = data['fl_date'].dt.day
    
    # Purge unused columns
    X = ppf.flight_test_features(data, purged=True)
    y = data[['arr_delay', 'is_delayed']]
    
    # Substitue mean delay values for categorical features
    X = week_month(df=X, time_period=time_period)
    
    # For flights of the first week on Jan 2019 that did not fly in the
    # month of 2018, set tail number mean to carrier mean
    X['tail_num_month_mean_arr_delay'].fillna(
        X['op_unique_carrier_month_mean_arr_delay'],
        inplace=True
    )
    
    # set nan to mean
    X = ppf.process_nan_values(
        df=X,
        features_to_mean=[
            'origin_month_mean_dep_delay',
            'dest_month_mean_arr_delay',
            'tail_num_month_mean_arr_delay',
            'op_unique_carrier_month_mean_arr_delay'
        ]
    )
    
    # Drop highly correlated features
    X.drop(['crs_dep_time',
            f'origin_city_name_{time_period}_mean_dep_delay',
            f'dest_city_name_{time_period}_mean_arr_delay'],
           axis=1,
           inplace=True)
    
    return X, y


def performance_stats(feature: 'str', groupby: 'str'):
    """
    
    """
    google_drive_path = ('~/Google Drive/My Drive/Lighthouse Labs/'
                         + 'Mid-term Project/Data-Jan/')
    
    files = {
        '2018' : google_drive_path + '2018-01.csv',
        '2019' : google_drive_path + '2019-01.csv'
    }
    
    
    frames = []

    for yr, file in files.items():
        data = load_and_process(csv_path=file, time_period='month')
        
        date_filters = {
            # First week of January
            'week' : ((data['fl_date'] >= f'{yr}-01-01') &
                (data['fl_date'] <= f'{yr}-01-07')),
            # Month of January
            'month' : ((data['fl_date'] >= f'{yr}-01-01') &
                (data['fl_date'] <= f'{yr}-01-31'))
        }
            
        for timeline, date_filter in date_filters.items():
            stats = pd.DataFrame()
            stats[[
                f'{yr}_{timeline}_{groupby}_mean_{feature}', 
                f'{yr}_{timeline}_{groupby}_std_{feature}', 
                f'{yr}_{timeline}_{groupby}_skew_{feature}'
            ]] = (data[date_filter][[feature, groupby]]
                  .groupby(by=[groupby])
                  .agg({feature : ['mean', 'std', 'skew']})
                 )
            
            frames.append(stats)
    
    stats = pd.concat(frames, axis=1)
    
    return stats

def save_stats():
    """
    
    """
    
    feature_dict = {
        'origin' : 'dep_delay',
        'origin_city_name' : 'dep_delay',
        'dest' : 'arr_delay',
        'dest_city_name' : 'arr_delay',
        'tail_num' : 'arr_delay',
        'op_unique_carrier' : 'arr_delay'
    }
    
    for k, v in feature_dict.items():
        stats = performance_stats(feature=v, groupby=k)
        stats.to_csv(f'../data/feature_average_delay_stats/{k}_{v}_stats.csv')
    
    return None
def daily_flight_order(df):
    
    """
    Returns the pandas dataframe ordered by [fl_date, tail_num, crs_dep_time] with an added column indicating how many flights that plane has undertaken previously during the same day.   
    """
    
    data = df.sort_values(['fl_date', 'tail_num', 'crs_dep_time']).reset_index()
    data['n_previous_flights'] = 0
    
    for table_index in range(1, data.shape[0]):
        if data.loc[table_index, 'tail_num'] != data.loc[table_index - 1, 'tail_num']:
            continue
        data.loc[table_index, 'n_previous_flights'] = data.loc[table_index - 1, 'n_previous_flights'] + 1
        
    return data


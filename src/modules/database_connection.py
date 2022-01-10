#!/usr/bin/env python
# coding: utf-8

# Connect to the mid_term_project PostgreSQL database


import psycopg2  # PostgreSQL database adapter
from psycopg2 import sql  # SQL string composition
import pandas as pd

# Project level modules
from modules.database_credentials import credentials  # PostgreSQL database credentials
from modules import sql_statements as sqs  # PostgreSQL statements


# [psycopg2 documentation](https://www.psycopg.org/docs/)


def postgresql_connection(db_credentials: 'str' = credentials):
    """
    Create a new database session
    
    Parameters:
    -----------
    db_credentials : string
        The credentials string format corresponds to psycopg2.connect()
        parameter format.
        Example: "dbname=test user=postgres password=secret"
        
    Returns:
    --------
    connection : (psycopg2 connection object)
        A PostgreSQL connection
    """
    
    # Make connection to PostgreSQL database with credentials
    connection = psycopg2.connect(db_credentials)
    print('Connected')
    return connection


def dataframe_to_csv(df, csv_path: str):
    """
    Save Pandas Dataframe to csv file
        
    Parameter
    ---------
    df : Pandas Dataframe
        query results
    csv_path : string
        filepath
        
    Returns
    -------
    None
    
    This works well after loading a PostgreSQL query result into memory.
    """
    
    # Write csv with header and without index column
    df.to_csv(csv_path, header=True, index=False)
    
    return None


def postgresql_to_csv(cursor, sql_statement: 'str', csv_path: 'str'):
    """
    Save PostgreSQL query results to csv file
    
    Parameters
    ----------
    cursor : psycopg2 cursor object
    sql_statement : string
        SQL statement
    csv_path : string
        filepath
    
    Returns
    -------
    None

    Adapted from ObjectRocket
    https://kb.objectrocket.com/postgresql/from-postgres-to-csv-with-python-910
    """
    
    # remove semicolon from query string
    S = sql_statement.replace(';', '')
    
    # SQL composition to prevent SQL injection
    SQL_for_file_output = (
        sql.SQL('COPY ({}) TO STDOUT WITH CSV HEADER')
        .format(sql.SQL(S))
    )
    
    # Write query results to csv
    with open(csv_path, 'w') as f_output:
        cursor.copy_expert(SQL_for_file_output, f_output)
    
    return None


def postgresql_results(connection,
                       query: 'str',
                       variables: 'tuple | None' = None):
    """
    Get PostgreSQL query results
    
    Parameters
    ----------
    connection : (psycopg2 connection object)
        A PostgreSQL connection
    query : string
        SQL query
    variables : tuple or None, default None
        Parameters to pass to SQL query
    
    Returns
    -------
    rows : list((tuples))
    column_names : list
    """
    
    with connection.cursor() as cursor: # client side cursor
        # execute sql statement
        cursor.execute(query=query, vars=variables)

        # Retrieve query column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Fetch all (remaining) rows of a query result
        rows = cursor.fetchall()
    
    return rows, column_names


def execute_sql_statement(connection,
                          query: 'str',
                          variables: 'tuple | None' = None,
                          save_to_csv: 'bool' = False,
                          csv_path: 'str | None' = None):
    """
    Returns a PostgreSQL query as a Pandas Dataframe
    
    Parameters
    ----------
    connection : (psycopg2 connection object)
        A PostgreSQL connection
    query : string
        SQL query
    variables : tuple or None, default None
        Parameters to pass to SQL query
    save_to_csv : bool, default False
        Write query result to csv
    csv_path : string or None, default None
        Filepath to save csv output
            
    Returns
    -------
    df : Pandas DataFrame
        dataframe of query results
    """
    
    # Get PostgreSQL query results and column names
    rows, column_names = postgresql_results(connection=connection,
                                            query=query,
                                            variables=variables)
    
    # Store query results in Pandas Dataframe
    df = pd.DataFrame(rows, columns=column_names)
    
    # If True and a file path is provided save the dataframe to csv
    if (save_to_csv) & (csv_path != None):
        dataframe_to_csv(df=df, path=csv_path)
    
    return df


def get_table_data_types(connection, table_name: str):
    """
    Returns a summary table of the columns and corresponding datatypes
    in a given PostgreSQL database table.
    
    Parameters
    ----------
    connection : psycopg2 connection object
        A PostgreSQL connection
    table_name : string
        Name of the table
        
    Returns
    -------
    df : Pandas Dataframe
    """
    
    # Properly formated SQL statement that prevents SQL injection
    query = """
    SELECT
       column_name,
       data_type
    FROM 
       information_schema.columns
    WHERE 
       table_name = (%s);
    """
    
    # store table_name as a tuple
    variables = (table_name,)
    
    # Get PostgreSQL query results and column names
    rows, column_names = postgresql_results(connection=connection,
                                            query=query,
                                            variables=variables)
    
    # Store query results in Pandas Dataframe
    df = pd.DataFrame(rows, columns=column_names)
    df.columns.name = table_name  # Set dataframe name
    
    return df


# Summary Statistics

# [How to derive summary statistics using PostgreSQL](https://towardsdatascience.com/how-to-derive-summary-statistics-using-postgresql-742f3cdc0f44)


def get_descriptive_statistics(connection,
                               stat_type: 'str num | cat',
                               save_to_csv: 'bool' = False,
                               csv_path: 'str | None' = None):
    """
    Get descriptive statistics for numeric or categorical columns
    in PostgreSQL database
    
    Paramaters
    ----------
    connection : psycopg2 connection object
        A PostgreSQL connection
    stat_type : string, 'num' or 'cat'
        Select type of descriptive statistic, numeric or categorical
    save_to_csv : bool, default False
        Write query result to csv
    csv_path : string or None, default None
        Filepath to save csv output
    
    Returns
    -------
    df : Pandas DataFrame
    """
    
    # Select descriptive statistic type
    if stat_type == 'num':
        query = sqs.numerical_statistics_sql
    elif stat_type == 'cat':
        query = sqs.categorical_statistics_sql
    
    # retrive descriptive statistics
    df = execute_sql_statement(connection,
                               query=query,                             
                               save_to_csv=save_to_csv,
                               csv_path=csv_path)
    
    if 'sno' in df.columns:
        df = df.drop('sno', axis=1)

    return df


if __name__ == '__main__':
    # Creat database connection
    with postgresql_connection() as connection:
    
        # Low consequence SQL statement for testing
        SQL_statement = """
        SELECT *
            FROM flights
            LIMIT 10;
        """

        postgresql_to_csv(sql_statement=SQL_statement, csv_path='../data/filename.csv')

        df = execute_sql_statement(connection=connection, query=SQL,
                               save_to_csv=True, csv_path='../data/test.csv')

        df = get_table_data_types(connection=connection, table_name='flights')


import pandas as pd
import pyodbc


def query_to_df(sql_file, driver='upiqm110'):

    # Read SQL
    sql_path = 'sql/{}.sql'.format(sql_file)
    sql_query = open(sql_path).read()

    # Connection
    print('Connecting to database ... ')
    con = pyodbc.connect('DSN={driver}'.format(driver=driver))    

    return pd.read_sql_query(sql_query, con)


# Olive Migration
def query_to_output(sql_file, output='csv', driver='upiqm110', chunksize=100000):
    """
    Olive migrate is a function which takes an olive schema and table as an argument
    then downloads the table into a local csv.

    @param driver: odbc name for driver.
    @param schema: schema of user.
    @param table: table to download.
    @param chunksize: How many rows per chunk to download from olive.
    @param output_file: location of output file.
        """

    # Read SQL
    sql_path = 'sql/{}.sql'.format(sql_file)
    sql_query = open(sql_path).read()

    # Connection
    print('Connecting to database ... ')
    con = pyodbc.connect('DSN={driver}'.format(driver=driver))

    # Create a dataframe list
    dfl = []

    # Create an empty dataframe
    dfs = pd.DataFrame()

    i=1
    # Read the SQL in chunks
    for chunk in pd.read_sql_query(sql_query, con, chunksize = chunksize):

        # Append chunks to the dataframe list
        dfl.append(chunk)
        print('Loaded chunk: ' + str(i))
        i += 1


    # Append from list to dataframe
    dfs = pd.concat(dfl, ignore_index=True)

    if (output=='csv'):
        output_file = sql_file
        output_path = 'data/{}.csv'.format(output_file)
        dfs.to_csv(output_path, index=False)
    elif(output=='dataframe'):
        return dfs
    else:
        print('Choose a correct option for output: csv or dataframe')




    



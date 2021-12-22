import os
import pandas as pd
import pyodbc


# Query to DF
def query_to_df(sql_file, driver='upiqm110'):

    # Read SQL
    sql_query = open(sql_file).read()

    # Connection
    print('Connecting to database ... ')
    con = pyodbc.connect('DSN={driver}'.format(driver=driver))    

    return pd.read_sql_query(sql_query, con)


def queries_to_dfl(folder):  
    file_list = [file for file in os.listdir(folder) if file.endswith(".sql")]
    
    dfl = []
    for file in file_list:
        sql_path = folder + '/' + file
        df = query_to_df(sql_path)
        dfl.append(df)
    return dfl


def dfl_to_excel(excel_file, dfl, sheet_names, startcol=1):
    excel_path = excel_file + '.xlsx'

    with pd.ExcelWriter(excel_path) as writer:
        for i, df in enumerate(dfl):
            df.to_excel(writer, sheet_names[i], index = False, startcol=startcol)
        writer.save()
        

def dir_to_excel(excel_file, query_dir, startcol=1):
    
    file_list = [file for file in os.listdir(query_dir) if file.endswith(".sql")]
    file_list_wo_extension = [file.rsplit('.', 1)[0] for file in file_list]
    sheet_names = file_list_wo_extension
    
    dfl = queries_to_dfl(query_dir)
    dfl_to_excel(excel_file, dfl, sheet_names, startcol=startcol)
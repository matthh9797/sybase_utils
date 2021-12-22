import datetime


def revive_tobedropped(
        date
        ,output_file='tobedropped.sql'
        ,table_list=[
        'PERM_Discovery_Plus'
        ,'PERM_Elf_talk_data'
        ,'PERM_VIP_MNI'
        ,'PERM_VIP_movie_mondays'
        ,'PERM_targeted_props_id'
    ]):
    """
    Function for moving tables from TOBEDROPPED to original name
    @param date add date on TOBEDROPPED table
    @param output_file name of output_file
    @param table_list list of tables in statement
    """

    template_sql = """drop table if exists {table_name};
    select * into {table_name} from TOBEDROPPED_{date}{table_name};
    drop table TOBEDROPPED_{date}{table_name};
    grant select on {table_name} to public;
    """
    dt = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    format_date = dt.strftime("%d_%m_%Y")
    sql_list=[]

    for table in table_list:
        sql_list = sql_list+[template_sql.format(table_name=table, date=format_date)]

    textfile = open(output_file, "w")
    for element in sql_list:
        textfile.write(element + "\n")
    textfile.close()

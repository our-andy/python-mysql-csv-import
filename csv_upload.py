import os
import pandas as pd
import mysql.connector
import logging
import math

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def csv_files():

    # get names of only csv files
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".csv"):
            csv_files.append(file)

    return csv_files


def create_df(csv_files):
    data_path = os.getcwd()+'/'

    # loop through the files and create the dataframe
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path+file)
        except UnicodeDecodeError:
            # if utf-8 encoding error
            df[file] = pd.read_csv(data_path+file, encoding="ISO-8859-1")
        logging.info("File ["+file+"] successfully created")

    return df


def clean_tbl_name(filename):

    # rename csv, force lower case, no spaces, no dashes
    clean_tbl_name = filename.lower().replace(" ", "").replace(
        "-", "_").replace(r"/", "_").replace("\\", "_").replace("$", "").replace("%", "")

    tbl_name = '{0}'.format(clean_tbl_name.split('.')[0])

    return tbl_name


def clean_colname(dataframe):

    # force column names to be lower case, no spaces, no dashes
    dataframe.columns = [x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_").replace(
        "\\", "_").replace(".", "_").replace("$", "").replace("%", "") for x in dataframe.columns]

    # processing data
    replacements = {
        'timedelta64[ns]': 'varchar(100)',
        'object': 'varchar(100)',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(
        dataframe.columns, dataframe.dtypes.replace(replacements)))

    return col_str, dataframe.columns

def upload_to_db(host, database, user, password,
                 tbl_name, col_str, dataframe):

    try:
        conn = mysql.connector.connect(
            host=host, database=database, user=user, password=password)
        cursor = conn.cursor()
        cursor.execute("drop table if exists %s;" % (tbl_name))
        cursor.execute("create table %s (%s);" % (
            tbl_name, col_str))
        logging.info("Table ["+tbl_name+"] successfully created")
        data_list = dataframe.to_numpy().tolist()
        for i in range(0, len(data_list)):
            row_values = convert_list(data_list[i])

            sql_statement = 'INSERT INTO %s VALUES (%s);' % (
                tbl_name, row_values)

            cursor.execute(sql_statement)
        conn.commit()
        cursor.close()
        logging.info(
            "["+str(len(data_list)) + "] rows successfully inserted into table [" + tbl_name + "]")
    except mysql.connector.Error as err:
        logging.info("Exception: {}".format(err))


def convert_list(list):
    base_string = ""

    for item in list:
        if isinstance(item, str):
            base_string = base_string + '\"' + item + '\"' + ","
        elif math.isnan(item):
            base_string = base_string + 'NULL' + ","
        else:
            base_string = base_string + str(item) + ","

    return base_string[:-1]


def symbol_check(element):
    print

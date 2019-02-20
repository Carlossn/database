############################### DATABASE CLASS#############################
########################################################################### 

## Follow Instructions

## 1) Download MySQL open source version: https://dev.mysql.com/downloads/installer/
# https://downloads.mysql.com/docs/mysql-installation-excerpt-8.0-en.a4.pdf
# Select "web"  MySQL Installer package option:
# when installing only select server option and don't add other mysql products
## 2) Python needs a MySQL driver to access the MySQL database:
# pip install mysql-connector
#
# Tutorial Links:
# https://www.w3schools.com/python/python_mysql_getstarted.asp
# http://www.mysqltutorial.org/python-mysql/
# 
# This library assumes a database is created with the next tables:
# i) 	ata_vendor: data vendor information.
# ii) 	exchange: exchange information.
# iii) 	symbol: tickers info.
# iv) 	daily_price: yahoo price time series info for each ticker from "symbol" table 
#
# Check the notebook in github demonstrating how to create the database and these tables: 

import mysql.connector
from configparser import ConfigParser
import os
import mysql.connector
import datetime as dt
import pytz
import pandas as pd
import numpy as np


###################################### AUXILIARY CLASSES AND METHODS ###############################################
def replace_(text, old, new):
    '''
    replace multiple old strings at once
    Params
    ------
    text = string. Entire text where the (old) characters to be replaced are.
    old = list of strings in old to be replaced
    new = string
    '''    
    for o in old:
        text = text.replace(o, new)
    return text

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types 
	This converter will be activated after connecting with the code line:
    db.set_converter_class(NumpyMySQLConverter) 

    """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)
        

###################################### CONNECTING METHODS ###############################################

def connection_remote_config_file(path ,filename='config.ini', section='mysql', dbname=None):
    ''' 
    Read database configuration file and return a dictionary object
    Param
    -----
    path: string with the windows location of the config file containing mysql's host, user and password details.
    e.g. "C:\\Users\\User_name\\Desktop\\SSH_KEYS"
    filename: string name of the configuration file. Default is finding a file named "config.ini"
    section: section of database configuration. Default is calling that section within the config file as "mysql"
    dbname: string name of database name to connect. Default is None.
    '''
  
    # Move to the path to find the config file:
    origin= str(os.getcwd())
    os.chdir(path)
    # create parser and read ini configuration file:
    parser = ConfigParser()
    parser.read(filename)
    # get section, default to mysql:
    mylog = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            mylog[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))
    # enter log details and connect with sql server:
    if dbname!=None:
        mydb = mysql.connector.connect(host=mylog['host'], user=mylog['user'],
        	password=mylog['password'], database=dbname)
    else:
        mydb = mysql.connector.connect(host=mylog['host'], user=mylog['user'], password=mylog['password'])
        
    os.chdir(origin) # go back to inception path
    mydb.set_converter_class(NumpyMySQLConverter) # allow python float and int types to be accepted by mysql
    return mydb # connector object

def connection_details(connector_name):
    '''
    Returns connection status and current session login details
    Params
    ------
    Connector_name = string. Name of the mysql.connector.connect() object.
    '''
    db = eval(connector_name)
    print('Connection check: ', db.is_connected())
    print('Server Host: ', db.server_host)
    print('Database:', db.database)
    print('User: ', db.user)
    print('Server Port: ', db.server_port)
    print('Connection ID: ', db.connection_id)
    print('Unix Socket: ', db.unix_socket)
    print('Server Connection Character Set: ', db.charset)
    print('Python Connection Character Set: ', db.python_charset)

###################################### AUXILIARY QUERY METHODS ###############################################

def SHOW_DATABASES(connector_name):
    '''
    Return databases in sql created up to date
    Params
    ------
    Connector_name = string. Name of the mysql.connector.connect() object.
    '''
    conn= eval(connector_name)
    dbcursor = conn.cursor(buffered=True)
    dbcursor.execute('SHOW DATABASES')
    for x in dbcursor:
        print(x) 

def query_with_fetchall(connection_name, query, get_object=False):
    '''
    Print rows from all the query output list return when using cursor.fetchall()
    Params
    -------
    connection_name = string. Object name of the db connection object.
    query: string. SQL syntax query.
    get_object: boolean. False default. if true, it will return a python object with query results
    '''
    from mysql.connector import Error
    conn = eval(connection_name)
    cursor_ = conn.cursor(buffered=True)
    query_list=[]
    try:
        cursor_.execute(query)
        rows = cursor_.fetchall()
 
        print('Total Row(s):', cursor_.rowcount)
        for row in rows:
            print(row)
    
    except Error as e:
        print(e)
    if get_object==True:
        return rows

def query_with_fetchmany(connection_name,query, size_, get_object=True):
    '''
    Print only n rows from all the query output. 
    Params
    ------
    connection_name = string. Object name of the db connection object.
    query: string. SQL syntax query.
    size: integer. Number of rows from the query output to be displayed.
    get_object: boolean. False default. if true, it will return a python object with query results

    '''
    from mysql.connector import Error

    conn = eval(connection_name)
    cursor_ = conn.cursor(buffered=True)

    try:
        cursor_.execute(query)
        rows = cursor_.fetchmany(size_)
        
        for row in rows:
            print(row)
 
    except Error as e:
        print(e)
    if get_object==True:
        return rows

###################################### FEEDING TABLES WITH DATA###############################################

def insert_data_vendor(data_vendor_id, name, website_url, support_email, created_date, 
						path , filename='config.ini', section='mysql', dbname=None,):
    '''
    Insert a new vendor in "data_vendor" table
    Params
    ------
    data_vendor_id: integer. e.g. 1 for yahoofinance
    name: string.
    website_url: string.
    support_email: string. 
    created_date: format 'YYYY-MM_DD'. First time a vendor is entered into the db. 
    path: string with the windows location of the config file e.g. "C:\\Users\\User_name\\Desktop\\SSH_KEYS"
    filename: string name of the configuration file. Default is finding a file named "config.ini"
    section: section of database configuration. Default is calling that section within the config file as "mysql"
    dbname: string name of database name to connect. Default is None.

    '''
    # Connect with db and create cursor:
    db = connection_remote_config_file(path,filename, section, dbname)
    dbcursor = db.cursor(buffered=True)
    #  retrieve fields from "data_vendor" table
    dbcursor.execute('DESCRIBE data_vendor')
    des=dbcursor.fetchall()
    cols = [x[0] for x in des]
    #automate query
    query = 'INSERT INTO data_vendor('+str(cols)+') VALUES('+('%s,'*len(cols))[:-1]+')'
    query = replace_(query,['[',']','\''],'')
    # Execute query:
    dbcursor.execute(query, [data_vendor_id, name, website_url,support_mail, created_date, dt.date.today()])
    # Commit query to server db (save changes) and close db connection 
    db.commit()
    db.close()    


def insert_exchange(exchange_id, abbrev, name, city, country, currency, created_date,
    path ,filename='config.ini', section='mysql', dbname=None, timezone='America/New_York'):
    
    '''
    Insert a new exchange in "exchange" table
    Params
    ------
    exchange_id: string. e.g. US
    abbrev: string. e.g. 'NYSE
    name: string. e.g. New York Stock Exchange
    city: string. e.g. New York
    country: string. e.g. US
    currency: string. e.g. USD
    created_date: format 'YYYY-MM_DD'. First time an exchange is entered into the db. 
    path: string with the windows location of the config file e.g. "C:\\Users\\User_name\\Desktop\\SSH_KEYS"
    filename: string name of the configuration file. Default is finding a file named "config.ini"
    section: section of database configuration. Default is calling that section within the config file as "mysql"
    dbname: string name of database name to connect. Default is None.
    timezone_offset: default "America/New_York". For others do:
        import pytz
        pytz.all_timezones


    '''
    # Connect with db and create cursor:
    db = connection_remote_config_file(path,filename, section, dbname)
    dbcursor = db.cursor(buffered=True)
    #  retrieve fields from "exchange" table
    bcursor.execute('DESCRIBE exchange')
    des=dbcursor.fetchall()
    cols = [x[0] for x in des]
    #automate query
    query = 'INSERT INTO exchange('+str(cols)+') VALUES('+('%s,'*len(cols))[:-1]+')'
    query = replace_(query,['[',']','\''],'')
    # Timezone translation:
    tz_ = pytz.timezone(timezone)
    tz_dt= dt_now = dt.datetime.now(tz=tz_)
    # Execute query:
    dbcursor.execute(query, [exchange_id, abbrev,name ,city, country,currency, tz_dt, created_date, dt.date.today()])
    # Commit query to server db (save changes) and close db connection 
    db.commit()
    db.close()    

def insert_symbol(file_name, created_date, path ,filename='config.ini', section='mysql', dbname=None):
    '''
    Insert a new symbol(s) in "symbol" table
    Params
    ------
    csv_file_name: string. e.g.'ticker.csv'
    created_date: format 'YYYY-MM_DD'. First time an exchange is entered into the db. 
    path: string with the windows location of the config file e.g. "C:\\Users\\User_name\\Desktop\\SSH_KEYS"
    filename: string name of the configuration file. Default is finding a file named "config.ini"
    section: section of database configuration. Default is calling that section within the config file as "mysql"
    dbname: string name of database name to connect. Default is None.
    '''
    # data import:
    data = pd.read_csv(file_name)
    # Connect with db and create cursor:
    db = connection_remote_config_file(path,filename, section, dbname)
    dbcursor = db.cursor(buffered=True)
    #  retrieve fields from "symbol" table
    dbcursor.execute('DESCRIBE symbol')
    des=dbcursor.fetchall()
    cols = [x[0] for x in des]
    # Enter changes:
    data['cik_id'] = data['cik_id'].apply(str) 
    data['created_date'] = created_date 
    data['last_updated_date'] = str(dt.date.today())     
    #automate query
    query = 'INSERT INTO symbol('+str(cols)+') VALUES('+(' %s,'*len(cols))[:-1]+')'
    query = replace_(query,['[',']','\''],'')
    # create param_list for "data" param in dbcursor.executemany(query,data)
    param_list=[]
    for i in range(0,len(data)-1):
        data_d = dict(data.loc[i,:])
        data_t = tuple([data_d[x] for x in cols]) # use cols from our symbol query to get the right order from the sp500 imported data
        param_list.append(data_t)
    # Execute query:
    dbcursor.executemany(query, param_list)
    # Commit query to server db (save changes) and close db connection 
    db.commit()
    db.close()    

def insert_price_data_yahoo(ticker_list, created_date, path , start='1999-12-31',end='2018-12-31', filename='config.ini', section='mysql', dbname=None):
    '''
    Insert yahoo price data into daily_price table into database
    Params
    -----
    ticker_list = List of tickers to be retrieved.
    created_date = Date of updated entry. Introduce today date as format 'YYYY-MM-DD'.
    path: string with the windows location of the config file e.g. "C:\\Users\\User_name\\Desktop\\SSH_KEYS"
    start = start date to retrieve data. Format 'YYYY-MM-DD'.
    end = end date to retrieve data. Format 'YYYY-MM-DD'.
    filename: string name of the configuration file. Default is finding a file named "config.ini"
    section: section of database configuration. Default is calling that section within the config file as "mysql"
    dbname: string name of database name to connect. Default is None.

    
    '''
    import pandas_datareader.data as web
    import datetime as dt

    # Gather daily_price fields:
    db = connection_remote_config_file(path ,filename, section, dbname)
    dbcursor = db.cursor(buffered=True)
    dbcursor.execute('DESCRIBE daily_price')
    des=dbcursor.fetchall()
    cols = [x[0] for x in des]
    cols=cols[1:] # 1st field "id" in cols is an Auto Increment field so it doesn't need to be part of the query
    db.close() # close db connection
    #automate query
    query = 'INSERT INTO daily_price('+str(cols)+') VALUES('+(' %s,'*len(cols))[:-1]+')'
    query = replace_(query,['[',']','\''],'') # replace_ is an auxiliary method

    for i in ticker_list:
        # get data and transform:
        i_yahoo = i[:-2].strip() # ticker only excluding MKT so that yahoo can recognizr it e.g. MMM US  => MMM
        df = web.DataReader(i_yahoo, 'yahoo', start, end)
        df['price_date'] = list(map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'),df.index)) # otherwise mysql doesn't recognize timestamp type
        df['ticker_ex'] = i
        df['created_date'] = '2019-02-14' 
        df['last_updated_date'] = str(dt.date.today())
        df['data_vendor_id']= str(1)
        df.rename(columns={'Open':'open_price','High':'high_price','Low':'low_price',
                       'Close':'close_price','Adj Close':'adj_close_price', 'Volume':'volume'}, inplace=True)
        # obtain ticker data rows (dates) stored as param for insert sql query
        param_= []
        for l in range(0,len(df)-1):
            param = tuple([df.iloc[l,:][x] for x in cols]) # use cols from our daily_price query to get the right order from df dowloaded data. 
            param_.append(param)
        # reconnect db:
        db = db.reconnect(attempts=3) # connect back 
        db.set_converter_class(NumpyMySQLConverter) # allow python float and int to be accepted by mysql
        dbcursor = db.cursor(buffered=True) 
        # insert ticker price info into db:
        try:
            dbcursor.executemany(query, param_)
        except:
            db.reconnect(attempts=3)
            db.set_converter_class(NumpyMySQLConverter)
            dbcursor.executemany(query, param_)        
        # Commit to server db (save changes) and close db connection before looping to next ticker
        db.commit()
        db.close()
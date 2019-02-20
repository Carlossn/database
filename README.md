# Financial securities database using MySQL and Python.

The notebook and py files in this repository allows the  user to create and retrieve a SQL database with S&P 500 members info as of February 2019.

Firstly, use the notebook as a guide to create the skeleton of the database (SQL schema and creation of 4 key tables daily_price, symbol, data_vendor and exchange)

Secondly, it is straightforward to insert new data into the database and retrieve it using the methods included in mysql_database.py file and SQL syntax.   

**Warning 1:** ensure MySQL is running always in the background in order for the connection with Python to be successful.
**Warning 2:** csv file includes SP500 ticker data but the user can upload any other securities (e.g. ETFs)

### Prerequisites
Please check notebook attached to install mySQL and Python 3.7 or higher installed.

Libaries required:

* mysql.connector
* configparser import ConfigParser
* os
* datetime
* pytz
* pandas
* numpy 

### Development Environment
* Python 3.7.0
* MySQL 8.0


import mysql.connector
from mysql.connector import errorcode

try:
    conn = mysql.connector.connect(user='root', host = '127.0.0.1', database = 'backend', unix_socket = '/var/run/mysqld/mysql.sock')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Something is wrong with your user name or password')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('Database does not exists.')
    else:
        print(err)

conn.close()


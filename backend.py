import mysql.connector
from mysql.connector import errorcode


"""Connecting to the database."""
try:
    conn = mysql.connector.connect(user = 'root', host = '127.0.0.1', database = 'backend', unix_socket = '/var/run/mysqld/mysql.sock')
    cursor = conn.cursor()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Something is wrong with your user name or password.')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('Database does not exists.')
    else:
        print(err)


"""Parsing csv file to dict(list()) structure."""
csv_dict = dict() 
csv_dict['Data'] = [] 
csv_dict['Info'] = [] 
csv_dict['Sum'] = [] 
with open('payments.csv') as f: 
    csv_reader = csv.DictReader(f, delimiter=',') 
    header = true
    for row in csv_reader: 
        if index(row) == 0:
            header = false
            csv_dict.fromkeys(row) 
        else: 
            csv_dict['Data'].append(row['Data']) 
            csv_dict['Info'].append(row['Info']) 
            csv_dict['Sum'].append(row['Sum']) 




conn.close()


import mysql.connector
from mysql.connector import errorcode
import csv
import re


"""Connecting to the database."""
try:
    conn = mysql.connector.connect(user = 'dmitry', host = '127.0.0.1', database = 'backend', unix_socket = '/var/run/mysqld/mysqld.sock')
    cursor = conn.cursor()
# FIX THIS SHIT
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Something is wrong with your user name or password.')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('Database does not exists.')
    else:
        print(err)
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^

"""Parsing csv file to dict(list()) structure."""
# Dictionary with all the csv table.
csv_dict = dict()
# Fill the header of the table.
csv_dict['Date'] = [] 
csv_dict['Info'] = [] 
csv_dict['Sum'] = [] 
with open('payments.csv') as f: 
    csv_reader = csv.DictReader(f, delimiter=',') 
    header = True
    for row in csv_reader: 
        # KOSTIL'!!!!!!!!
        if header:
            header = False
        else: 
            csv_dict['Date'].append(row['Date']) 
            csv_dict['Info'].append(row['Info']) 
            csv_dict['Sum'].append(row['Sum']) 


"""Collecting all INNs from DB."""
SQL_INN = 'SELECT inn FROM users'
cursor.execute(SQL_INN)
# Raw INNs from database (becomes in a list(tuple()) structure).
db_raw_inns = cursor.fetchall()
# Normalize structure (to list).
db_clean_inns = []
for i in range(len(db_raw_inns)):
    db_clean_inns.append(db_raw_inns[i][0])


"""Collecting all INNs from csv file."""
# INNs from csv file (becomes a full 'Info' string).
csv_raw_inns = []
for raw_str in csv_dict['Info']:
    csv_raw_inns.append(raw_str)


"""Bring it to normal."""
# Find db_clean_inns in csv_raw_inns (now list(Match object)).
csv_almost_clean_inns = [] 
for i in range(len(csv_raw_inns)): 
    for j in range(len(db_clean_inns)): 
        if re.search(db_clean_inns[j], csv_raw_inns[i]) != None: 
            csv_almost_clean_inns.append(re.search(db_clean_inns[j], csv_raw_inns[i]))

# Normalized csv_clean_inns (became to list()).
csv_clean_inns = []
for i in csv_almost_clean_inns:
    csv_clean_inns.append(i.group(0))


"""Compare two lists to get interceptions."""
# Dict with INNs as keys and booleans as values. If INN in both lists -> True.
result = dict.fromkeys(db_clean_inns)
for i in csv_clean_inns:
    for j in db_clean_inns:
        if j in i:
            result[j] = True
            
conn.close()

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

"""==========PARSING==========="""

"""Parsing csv file to dict(list()) structure."""
# Dictionary with all the csv table.
csv_dict = dict()
# Fill the header of the table.
csv_dict['Date'] = []
csv_dict['Info'] = []
csv_dict['Sum'] = []
# Reading file.
with open('payments.csv') as f: 
    csv_reader = csv.DictReader(f, delimiter=',') 
    header = True
    # Filling the csv_dict.
    for row in csv_reader: 
        # KOSTIL'!!!!!!!!
        if header:
            header = False
        else: 
            csv_dict['Date'].append(row['Date']) 
            csv_dict['Info'].append(row['Info']) 
            csv_dict['Sum'].append(row['Sum']) 


"""Collecting all INNs from DB.
Creating dict(list()) structure from DB. Collecting all INNs from DB."""
SQL_TEST = 'SELECT * FROM users'
cursor.execute(SQL_TEST)
db_raw = cursor.fetchall()
# Dict with structured info from DB.
db_dict = dict()
# List with INNs from DB.
db_clean_inns = []
for k in range(len(db_raw)):
    db_dict.setdefault(db_raw[k][2])
    db_dict[db_raw[k][2]] = list()
    for i in range(3):
        db_dict[db_raw[k][2]].append(db_raw[k][i])

    db_clean_inns.append(list(db_dict.keys())[k])


"""Collecting all INNs from csv file."""
# INNs from csv file (becomes a full 'Info' string).
csv_raw_inns = []
for raw_str in csv_dict['Info']:
    csv_raw_inns.append(raw_str)


"""Bring it to normal."""
# Find db_clean_inns in csv_raw_inns.
csv_clean_inns = [] 
for i in range(len(csv_raw_inns)): 
    for j in range(len(db_clean_inns)): 
        if re.search(db_clean_inns[j], csv_raw_inns[i]) != None: 
            csv_clean_inns.append((re.search(db_clean_inns[j], csv_raw_inns[i])).group(0))


"""Compare two lists to get interceptions."""
# Dict with INNs as keys and booleans as values. If INN in both lists -> True.
result = dict.fromkeys(db_clean_inns)
for i in csv_clean_inns:
    for j in db_clean_inns:
        if j in i:
            result[j] = True
            
conn.close()

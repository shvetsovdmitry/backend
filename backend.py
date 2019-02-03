#!/usr/bin/env python3
import re
import csv
import mysql.connector
from mysql.connector import errorcode
import datetime
from termcolor import colored
import time


"""Connecting to the database."""
try:
    conn = mysql.connector.connect(
                                user='dmitry',
                                host='127.0.0.1',
                                database='backend',
                                unix_socket='/var/run/mysqld/mysqld.sock'
                                )
    cursor = conn.cursor()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('[{0}] - {1}'.format(colored('ERROR!', 'red'),
                                   colored('Wrong user name or password!',
                                           'pink')))
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('[{0}] - {1}'.format(colored('ERROR!', 'red'),
                                   colored('Database does not exists!',
                                           'pink')))
    else:
        print('[{0}] - {1}'.format(colored('ERROR!', 'red'),
                                   colored('An error has occured!',
                                           'pink')))

if conn.is_connected():
    print('[--{0}--] - {1}'.format(colored('OK', 'green'),
                                   colored('Successfully connected to MySQL!',
                                           'yellow')))



"""1. Спарсить данные из входного файла."""

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


"""csv_dict = dict()
with open('payments.csv') as f:
    csv_reader = csv.DictReader(f, delimiter=',')
    for row in csv_reader:
        if len(csv_dict.keys()) == 0:
            csv_dict = csv_dict.fromkeys(csv_reader.fieldnames)
            for i in list(csv_dict.keys()):
                csv_dict[i] = []
        else:
            csv_dict['Date'].append(row['Date'])
            csv_dict['Info'].append(row['Info'])
            csv_dict['Sum'].append(row['Sum'])"""

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
    for i in range(4):
        db_dict[db_raw[k][2]].append(db_raw[k][i])

    db_clean_inns.append(list(db_dict.keys())[k])


"""Collecting all INNs from csv file."""
# INNs from csv file (becomes a full 'Info' string).
csv_raw_inns = []
for raw_str in csv_dict['Info']:
    csv_raw_inns.append(raw_str)


"""Bring it to normal. Find db_clean_inns in csv_raw_inns."""
# List with clean INNs from csv file.
csv_clean_inns = []
for i in range(len(csv_raw_inns)):
    for j in range(len(db_clean_inns)):
        if re.search(db_clean_inns[j], csv_raw_inns[i]) is not None:
            result = (re.search(db_clean_inns[j], csv_raw_inns[i])).group(0)
            if db_dict[result][3] == 0:
                csv_clean_inns.append(result)

"""2. Для каждой строки по ИНН (подразумевается,
что ИНН состоит из 12 цифр и всегда присутствует в столбце “Информация”)
найти пользователя, от которого поступила оплата."""

"""Compare two set(list)'s to get interceptions."""
# Set with paid users found in csv file via intersection operation.
paid_users = set(csv_clean_inns) & set(db_clean_inns)
"""# Output paid users INNs.
print('Users who paid:')
for i in range(len(paid_users)):
    print('{0}: {1}'.format(i + 1, list(paid_users)[i]))"""


"""3. Если пользователь найден в базе и активен,
то заносим данные об оплате (id пользователя, дата, сумма)
в таблицу платежей в БД. Если ИНН не найден или пользователь неактивен,
данные не заносятся."""

"""# Find if account active then write payment info in DB.
for key in list(db_dict.keys()):
    print(db_dict[key][3])"""


"""Поиск по совпадению в таблице csv.
Если пользователь активен и найден - записываем инфо об оплатах в БД.
Если пользователь не найден - записываем "Пользователь с таким ИНН не найден."
Если пользователь неактивен - записываем "Пользователь неактивен."."""


"""Creating dict with keys from paid_users list."""
coincidences = dict.fromkeys(paid_users)
# Fill the dict with empty lists
for i in paid_users:
    coincidences[i] = []


"""Find all coincidences with paid users."""
for i in range(len(csv_dict['Info'])):
    for j in range(len(paid_users)):
        if re.search(list(paid_users)[j], csv_dict['Info'][i]) is not None:
            if db_dict[list(paid_users)[j]][3] == 0:
                single_coincidence = []
                # Parsing date from csv_dict to YYYY-MM-DD format.
                splitted_date = csv_dict['Date'][i].split('/')
                cur_date = datetime.date(
                    int(splitted_date[2]),
                    int(splitted_date[0]),
                    int(splitted_date[1])
                    ).strftime('%Y-%m-%d')
                single_coincidence.append(cur_date)
                single_coincidence.append(csv_dict['Info'][i])
                single_coincidence.append(csv_dict['Sum'][i])
                coincidences[list(paid_users)[j]].append(single_coincidence)


"""Find the last index in payments table."""
cursor.execute('SELECT id FROM payments')
ids = list(cursor.fetchall())
index = len(ids)


"""Write coincidences to DB in payments table."""
for k, v in coincidences.items():
        for i in range(len(v)):
            try:
                # Insert into payments table values (id, INN, date, sum).
                cursor.execute(
                    "INSERT INTO payments VALUES (%s, %s, %s, %s)",
                    (
                        index, k, v[i][0],
                        float(v[i][2].replace(',', '.')
                              if re.search(',', v[i][2]) is not None
                              else v[i][2])
                    )
                )
                conn.commit()
                print('[{0}] - {1}'.format(
                    colored('STATUS', 'yellow'),
                    colored('Writing to DB <- {}'.format(k), 'green')))
                index += 1
            except mysql.connector.Error as error:
                conn.rollback()
                print('[{0}] - {1}'.format(
                                    colored('ERROR!', 'red'),
                                    colored(error, 'pink')))


if conn.is_connected():
    cursor.close()
    conn.close()
    print('[--{0}--] - {1}'.format(colored('OK', 'green'),
                                   colored('Connection to MySQL closed!',
                                           'yellow')))

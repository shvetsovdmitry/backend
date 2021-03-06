#!/usr/bin/env python3
import re
import csv
import mysql.connector
from mysql.connector import errorcode
import datetime
from termcolor import colored
import time
import argparse


def connect_to_db(user_name: str, db_name: str):
    """Connecting to the database."""
    db_conn = None
    try:
        db_conn = mysql.connector.connect(
                                    user=user_name,
                                    host='127.0.0.1',
                                    database=db_name if db_name is not None
                                    else 'backend',
                                    unix_socket='/var/run/mysqld/mysqld.sock'
                                    )
        cursor = db_conn.cursor()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print_msg('error', 'Wrong user name or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print_msg('error', 'Database does not exists!')
        else:
            print_msg('error', 'An error has occured!')

    if db_conn.is_connected():
        print_msg('ok', 'Successfully connected to MySQL!')
        return db_conn


# Init timer.
start_timer = time.time()

parser = argparse.ArgumentParser(description="""Parsing file with payments
                                 info and make changes in database.""")
parser.add_argument('-u', '--user', required=True, help='User name for MySQL.')
parser.add_argument('-d', '--database', required=False,
                    help='Database name ("backend" by default).')
parser.add_argument('-f', '--file', required=False,
                    help="""CSV file with payments info
                    ("payments.csv" by default).""")
parser.add_argument('-o', '--output', required=False,
                    help='Name of output file.')
args = vars(parser.parse_args())


def print_msg(status: str, msg: str):
    """Print status messages.
    status = 'ok', 'status' or 'error'."""
    if status == 'ok':
        print('[--{0}--] - {1}'.format(colored('OK', 'green'),
                                       colored(msg, 'yellow')))
    elif status == 'error':
        print('[{0}] - {1}'.format(colored('ERROR!', 'red'),
                                   colored(msg, 'pink')))
    elif status == 'status':
        print('[{0}] - {1}'.format(colored('STATUS', 'yellow'),
                                   colored(msg, 'green')))
    elif status == 'time':
        print('[-{0}-] - {1}'.format(colored('TIME', 'magenta'),
              'Execution time -> {0}'.format(msg)))


conn = connect_to_db(args['user'], args['database'])
cursor = conn.cursor()


"""1. Спарсить данные из входного файла."""


# Reading file.
def create_csv_dict(file_name: str):
    """Parsing csv file to dict(list()) structure."""
    csv_temp_dict = dict()
    with open('payments.csv' if file_name is None else file_name) as f:
        csv_reader = csv.DictReader(f, delimiter=',')
        for row in csv_reader:
            # Filling the header of the table.
            if len(csv_temp_dict.keys()) == 0:
                csv_temp_dict = csv_temp_dict.fromkeys(csv_reader.fieldnames)
                for i in list(csv_temp_dict.keys()):
                    csv_temp_dict[i] = []
            # Filling the csv_dict.
            else:
                csv_temp_dict['Date'].append(row['Date'])
                csv_temp_dict['Info'].append(row['Info'])
                csv_temp_dict['Sum'].append(row['Sum'])
    return csv_temp_dict


# Dictionary with all the csv table.
csv_dict = create_csv_dict(args['file'])


"""Collecting all INNs from DB.
Creating dict(list()) structure from DB. Collecting all INNs from DB."""
SQL_QUERY = 'SELECT * FROM users'
cursor.execute(SQL_QUERY)
db_raw = cursor.fetchall()
# Dict with structured info from DB.
db_dict = {}
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
not_paid_users = set(db_clean_inns) - set(csv_clean_inns)


"""3. Если пользователь найден в базе и активен,
то заносим данные об оплате (id пользователя, дата, сумма)
в таблицу платежей в БД. Если ИНН не найден или пользователь неактивен,
данные не заносятся."""

"""Creating dict with keys from paid_users list."""
coincidences = dict.fromkeys(paid_users)
# Fill the dict with empty lists
for i in paid_users:
    coincidences[i] = []


result = csv_dict.copy()
result['Result'] = [' ' for i in range(len(result['Date']))]

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
                result['Result'][i] = 'OK'
                coincidences[list(paid_users)[j]].append(single_coincidence)


"""Find not paid users and check his activity."""
for i in range(len(csv_dict['Date'])):
    if result['Result'][i] == ' ':
        subresult = []
        for j in range(len(paid_users)):
            if re.search(list(paid_users)[j], csv_dict['Info'][i]) is not None:
                subresult.append(True)
            else:
                subresult.append(False)
        if all(subresult) is False:
            for k in range(len(not_paid_users)):
                if re.search(list(not_paid_users)[k], csv_dict['Info'][i]) is not None:
                    if db_dict[list(not_paid_users)[k]][3] == 1:
                        result['Result'][i] = 'Пользователь не активен.'
                    else:
                        result['Result'][i] = 'Пользователь с таким ИНН не найден.'

for i in range(len(result['Result'])):
    if result['Result'][i] == ' ':
        result['Result'][i] = 'Пользователь с таким ИНН не найден.'


with open(args['output'] if args['output'] is not None else 'output.csv', 'w') as f:
    csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, escapechar=' ', quotechar='"')
    keys = list(result.keys())
    csv_writer.writerow([keys[0], keys[1], keys[2], keys[3]])
    for i in range(len(result['Date'])):
        csv_writer.writerow([result['Date'][i], result['Info'][i], result['Sum'][i], result['Result'][i]])


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
                print_msg('status', 'Writing to DB <- {}'.format(k))
                index += 1
            except mysql.connector.Error as error:
                conn.rollback()
                print_msg('error', error)


if conn.is_connected():
    cursor.close()
    conn.close()
    print_msg('ok', 'Connection to MySQL closed!')
    print_msg('time', '{0}s.'.format(round(time.time() - start_timer, 2)))

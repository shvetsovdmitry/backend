import re
import csv
import mysql.connector
from mysql.connector import errorcode

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
        if re.search(db_clean_inns[j], csv_raw_inns[i]) != None: 
            csv_clean_inns.append((re.search(db_clean_inns[j], csv_raw_inns[i])).group(0))


"""2. Для каждой строки по ИНН (подразумевается, 
что ИНН состоит из 12 цифр и всегда присутствует в столбце “Информация”) 
найти пользователя, от которого поступила оплата."""

"""Compare two set(list)'s to get interceptions."""
# Set with paid users found in csv file via intersection operation.
paid_users = set(csv_clean_inns) & set(db_clean_inns)
# Output paid users INNs.
print('Users who paid:')
for i in range(len(paid_users)):
    print('{0}: {1}'.format(i + 1, list(paid_users)[i]))


"""3. Если пользователь найден в базе и активен,
то заносим данные об оплате (id пользователя, дата, сумма) 
в таблицу платежей в БД. Если ИНН не найден или пользователь неактивен, 
данные не заносятся."""

# Find if account active then write payment info in DB.
for key in list(db_dict.keys()):
    print(db_dict[key][3])



"""Find the last index in """
cursor.execute('SELECT id FROM payments')
ids = list(cursor.fetchall())
index = len(ids)

"""Поиск по совпадению в таблице csv.
Если пользователь активен и найден - записываем инфо об оплатах в БД.
Если пользователь не найден - записываем "Пользователь с таким ИНН не найден."
Если пользователь неактивен - записываем "Пользователь неактивен."."""
for user in list(db_dict.keys()):
    if user in list(paid_users):
        if db_dict[user][3] == 0:
            print('Active paid user. Send to DB -> {}'.format(db_dict[user][2]))
            try:
                cursor.execute("""INSERT INTO payments VALUES (%s, %s, %s, %s)""", (index, db_dict[user][2], db_dict[user][0], db_dict[user][1]))
                conn.commit()
            except:
                conn.rollback()
            finally:
                index += 1
        else:
            print('Inactive user. Do not send to DB -> {}'.format(db_dict[user][2]))
    else:
        print('User payment not found. Do not send to DB -> {}'.format(db_dict[user][2]))




conn.close()
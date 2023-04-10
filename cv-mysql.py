import mysql.connector as mysql
import csv
import os

while True:
    try:
        password = input("Please enter your mysql password: ")
        mycom = mysql.connect(host='localhost',user='root',password=password)
    except:
        print("Please enter the correct password")
        continue
    break


cur = mycom.cursor()

cur.execute('show databases')
databases = [i[0].lower() for i in cur.fetchall()]
database = input("Please enter the database name:").lower()
while database not in databases:
    print("Please enter the correct name of database")
    database = input("Please enter the database name:").lower()
cur.execute(f'use {database}')


def is_float(n):
    try:
        float(n)
        return True
    except ValueError:
        return False


def check_leap_year(n):
    if n[2] % 100 == 0:
        if n[2] % 400 == 0:
            return True
        else:
            return False
    else:
        if n[2] % 4 == 0:
            return True
        else:
            return False


def check_if_csv_file_exist(csv_file_name):
    csv_files = [os.path.splitext(i)[0] for i in os.listdir(os.getcwd()) if os.path.splitext(i)[1]=='.csv']
    if csv_file_name in csv_files:
        return True
    else:
        print('Current working directory is',os.getcwd())
        return False
  
    

def is_date(n):
    try:
        n = list(map(int,n.split('-')))
        n = [n[i] for i in range(2,-1,-1)]
    except ValueError:
        return False
    if n[1] == 0 or n[2] == 0 or n[0] == 0:
        return False
    else:
        if check_leap_year(n):
            if n[1] > 12:
                return False
            elif n[1] == 2 and n[0] > 29:
                return False
            elif n[1] < 8 and n[1] % 2 == 0 and n[0] > 30:
                return False
            elif n[1] < 8 and n[1] % 2 != 0 and n[0] > 31:
                return False
            elif n[1] >= 8 and n[1] % 2 == 0 and n[0] > 31:
                return False
            elif n[1] >= 8 and n[1] % 2 != 0 and n[0] > 30:
                return False
        else:
            if n[1] == 2 and n[0] > 28:
                return False
            else:
                if n[1] > 12:
                    return False
                elif n[1] < 8 and n[1] % 2 == 0 and n[0] > 30:
                    return False
                elif n[1] < 8 and n[1] % 2 != 0 and n[0] > 31:
                    return False
                elif n[1] >= 8 and n[1] % 2 == 0 and n[0] > 31:
                    return False
                elif n[1] >= 8 and n[1] % 2 != 0 and n[0] > 30:
                    return False
    return True            


def correct(n):
    n = str(n).strip()
    ans = ''
    for i in n:
        if i in ['.',' ',',']:
            ans+='_'
        else:
            ans+=i
    return ans
            

def data_type_teller(n):
    if all(map(lambda x:str(x).isdigit(),n)):
        return "int"
    elif all(map(lambda x:is_float(str(x)),n)):
        return 'float'
    elif all(map(lambda x:is_date(str(x)),n)):
        return 'date'
    else:
        return 'varchar(2000)'
    

def table_in_mysql():
    cur.execute('show tables')
    data = cur.fetchall()
    return [i[0].lower() for i in data]



def csv_to_mysql():
    print('Current working database :',mycom.database)
    csv_file_name = input("Enter the file name :")
    table_name = csv_file_name


    while not check_if_csv_file_exist(csv_file_name):
        print("This csv file does not exist in current working directory")
        csv_file_name = input("Please enter the correct file name: ")

    while table_name  in table_in_mysql():
        table_name += '_'


    f = open(f'{csv_file_name}.csv','r')
    data = [i for i in csv.reader(f) if i!=[]]
    types = []
    column_name = list(map(correct,data[0]))
    data_ins = data[1:]

    for i in range(0,len(column_name)):     #Create a list for the column type
        column = [j[i] for j in data[1:]]
        column_type = data_type_teller(column)
        types.append(column_type)
    
    sen = f'Create table {table_name}('
    ins = f'insert into {table_name} values('
    for i in range(0,len(column_name)):
        if i != len(column_name)-1:
            sen += f'{column_name[i]} {types[i]},'
            ins += '%s,'
        else:
            sen += f'{column_name[i]} {types[i]})'
            ins += '%s)'
    cur.execute(sen)
    mycom.commit()
    cur.executemany(ins,data_ins)
    mycom.commit()
    print("The table is inserted in ",mycom.database)



def mysql_to_csv():
    print('Current working database :',mycom.database)
    table_name = input("Enter the table name please: ")

    while not table_name in table_in_mysql():
        print("This tables does not exist in database",mycom.database)
        table_name = input("Please enter the correct tablle name you wish to copy: ")
    csv_file_name = table_name

    while check_if_csv_file_exist(csv_file_name):
        csv_file_name += '_'
    
    f = open(f'{csv_file_name}.csv','w')
    writes = csv.writer(f)
    cur.execute(f'select * from {table_name}')
    column_name = tuple(map(lambda x:x.upper(),cur.column_names))
    writes.writerow(column_name)
    data = cur.fetchall()
    writes.writerows(data)


def copy_entire_database():
    print('Current working database :',mycom.database)
    ch = input("Would you like to change current database(Y/N):")
    if ch.lower().startswith('y'):
        cur.execute('show databases')
        data = [i[0] for i in cur.fetchall()]
        database = input('Enter the name of the database: ')
        while database not in data:
            print("This database doesnot exist")
            database = input("Please enter the correct database: ")
        cur.execute(f'use {database}')
        mycom.commit()
    else:
        database = mycom.database
    tables = table_in_mysql()
    path = os.getcwd()
    while os.path.isdir(database):
        database +='_'
    os.makedirs(database)
    path =  path+f'\{database}'
    os.chdir(path)
    for i in tables:
        f = open(f'{i}.csv','w')
        writes = csv.writer(f)
        cur.execute(f'select * from {i}')
        column_name = tuple(map(lambda x:x.upper(),cur.column_names))
        writes.writerow(column_name)
        data = cur.fetchall()
        writes.writerows(data)
    os.chdir('..')
    print("The folder is saved in",os.getcwd())


def copy_entire_directory():  #This function only copy csv file to the mysql
    print("Current working database is ",mycom.database)
    ch = input("Would you like to change current database (Y/N): ")

    if ch.lower().startswith('y'):
        cur.execute('show databases')
        data = [i[0] for i in cur.fetchall()]
        database = input('Enter the name of the database: ')
        while database not in data:
            print("This database doesnot exist")
            database = input("Please enter the correct database: ")
        cur.execute(f'use {database}')
        mycom.commit()

    csv_file = [os.path.splitext(i)[0].lower() for i in os.listdir() if os.path.splitext(i)[1]=='.csv']
    for i in csv_file:
        table_name = i
        while table_name  in table_in_mysql():
            table_name += '_'
        f = open(f'{i}.csv','r')
        data = [i for i in csv.reader(f) if i != []]
        type_date = []
        column_name = list(map(correct,data[0]))
        data_ins = data[1:]
        for k in range(0,len(column_name)):
            column = [j[k] for j in data[1:]]
            column_type = data_type_teller(column)
            type_date.append(column_type)
        sen = f'Create table {table_name}('
        ins = f'insert into {table_name} values('
        for k in range(0,len(column_name)):
            if k != len(column_name)-1:
                sen += f'{column_name[k]} {type_date[k]},'
                ins += '%s,'
            else:
                sen += f'{column_name[k]} {type_date[k]})'
                ins += '%s)'
        cur.execute(sen)
        mycom.commit()
        cur.executemany(ins,data_ins)
        mycom.commit()


def error():
    print("Please enter a proper command")


print("_______MAIN PROGRAM___________")
d = {'1':mysql_to_csv,'2':csv_to_mysql,'3':copy_entire_database,'4':copy_entire_directory,'5':quit}
ch = 0
while ch!='5':
    print('\n\n')
    print("1. Copy 1 table from mysql to csv file ")
    print('2. Copy 1 csv file to mysql ')
    print('3. Copy entire database and its relation to an seperate directory in csv format')
    print('4. Copy all the csv file present in the directory to mysql')
    print("5. For quiting the program")
    ch = input("Please enter your choice (1-5):")
    d.get(ch,error)()
    

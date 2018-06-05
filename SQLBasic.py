import sqlalchemy as sqla
import pymysql
import MySql as Database
import numpy as np
import csv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *

def main():
    try:
        engine = create_engine('mysql+pymysql://dwdtestuser:asdassaj14123@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')
        print("Engine creation successful")
        connection = engine.connect()
        print("Connection successful")
    except Exception as error:
        if "Engine not found" in str(error):
            print("Engine not found")
        else:
            print(error)
    #CALL query
    test = SQLpostcode(94501,connection)
    print(test)
    print("")
    print("")
    print("Zeile")
    print(test[0])
    print("Spalte")
    print(test[0][1])
    connection.close()

def SQLpostcode(postcode,connection):
    result = []
    for row in connection.execute(sqla.select([Database.Dwd]).where(Database.Dwd.postcode == postcode)):
        result.append(row)
    return np.vstack(result)

if __name__ == '__main__':
    main()

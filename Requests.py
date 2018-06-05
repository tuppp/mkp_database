import sqlalchemy as sqla
import pymysql
#import pysqlcipher3
from numpy import genfromtxt
import csv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
import MySql.Dwd as pymysql
import numpy as np


def query(postcode):
    array = []
    engine = create_engine('mysql+pymysql://dwdtestuser:asdassaj14123@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')
    conn = engine.connect()
    s = select([pymysql.Dwd]).where(pymysql.Dwd.postcode == postcode)
    result = conn.execute(s)
    for row in result:
        array = []
        for info in row:
            print(info)


def main():
    query(94501)


if __name__ == "__main__":
    main()


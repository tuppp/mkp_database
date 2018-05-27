import sqlalchemy as sqla
import pymysql
#import pysqlcipher3
from numpy import genfromtxt
import csv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *

dbname = 'pyWetter'
newDB = True
Base = declarative_base()

#https://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
class Dwd(Base):
    __tablename__ ='dwd'

    '''
    STATIONS_ID = Column(String(40))
    MESS_DATUM =  Column(String(40),primary_key=True)
    D_Windgesch = Column(String(40))
    NS_Menge = Column(Integer)
    NS_Art = Column(String(40))
    D_Temp = Column(Integer)
    '''
    station_id = Column(Integer,primary_key=True)
    station_name = Column(String(100))
    postcode = Column(Integer)
    measure_date = Column(Integer,primary_key=True) #TODO
    quality_1 = Column(Integer)
    max_wind_speed = Column(Float)
    average_wind_speed = Column(Float)
    quality_2 = Column(Integer)
    precipitation_amount = Column(Float)
    precipitation_type = Column(String(50))
    sun_hours = Column(Float)
    snow_height = Column(Float)
    coverage_amount = Column(Float)
    vapor_pressure = Column(Float)
    air_pressure = Column(Float)
    average_temp = Column(Float)
    relative_himidity = Column(Float)
    max_temp = Column(Float)
    min_temp = Column(Float)
    ground_min_temp = Column(Float)




def main():
    print("Test!")

    engine = create_engine('mysql+pymysql://dwdtestuser:asdassaj14123@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')
    #mysql --ssl-cipher=AES128-SHA -u dwdtestuser -p -h weather.service.tu-berlin.de dwdtest
    Base.metadata.create_all(engine)
    Session = sqla.orm.sessionmaker()
    Session.configure(bind=engine)
    se = Session()

    #TODO Pfad ändern
    Load_Data('C:\\Users\Vika\Documents\TU\ProgPraktikum\data.csv', engine,se)

    for date in se.query(Dwd.measure_date).filter(Dwd.average_temp < 15):
        print(date)

    se.close()

def queryExample(test):

    print()
    #for istance in se.query(User).order


def printTables(m):
    for table in m.tables.values():
        print(table.name)
        for column in table.c:
            print(column.name)

def Load_Data(file_name,engine, se):

    datafile = open(file_name, 'r')
    readCSV = csv.reader(datafile, delimiter=';')
    count = 0
    for row in readCSV:
        if count > 0:
            for i in range(len(row)):
                if row[i] == "nan" or row[i] == "None":
                    row[i] = sqla.sql.null()

            nrow = Dwd(**{
                'station_id' :row[0],
                'station_name' :row[1],
                'postcode' :row[2],
                'measure_date' :row[3],
                'quality_1' : row[4],
                'max_wind_speed' :row[5],
                'average_wind_speed' :row[6],
                'quality_2' :row[7],
                'precipitation_amount' :row[8],
                'precipitation_type' :row[9],
                'sun_hours' :row[10],
                'snow_height':row[11],
                'coverage_amount' :row[12],
                'vapor_pressure' :row[13],
                'air_pressure' :row[14],
                'average_temp' :row[15],
                'relative_himidity':row[16],
                'max_temp' :row[17],
                'min_temp' :row[18],
                'ground_min_temp' :row[19]
            })
            se.add(nrow)

            try:
                 se.commit()
            except sqla.exc.IntegrityError:
                # se.expunge(row)
                se.rollback()
                print(":/")
        count= count + 1

            # http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#querying


if __name__ == "__main__":
    main()


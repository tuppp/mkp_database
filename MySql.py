import sqlalchemy as sqla
import pymysql
#import pysqlcipher3
from numpy import genfromtxt
import csv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
import datetime


dbname = 'pyWetter'
newDB = True
Base = declarative_base()
all_table_names = {'websites','testwebsite','accuweathercom','openweathermaporg','wettercom','wetterde','wetterdienstde'}

class Websites(Base):
    __tablename__ = 'websites'
    websitename = Column(String(50), primary_key=True)
    url = Column(String(100))

class Website_Data():
    measure_date = Column(Integer,primary_key=True) #TODO
    measure_date_hour = Column(Integer)
    measure_date_prediction = Column(Integer, primary_key=True)  # TODO
    measure_date_prediction_hour = Column(Integer)
    postcode = Column(Integer)
    city = Column(String(50))
    temp = Column(Float)
    humidity_prob =Column(Float)
    precipitation_amount = Column(Float)
    precipitation_type = Column(String(50))
    wind_speed = Column(Float)
    air_pressure_ground = Column(Float)
    air_pressure_sea = Column(Float)
    max_temp = Column(Float)
    min_temp = Column(Float)
    sun_hours = Column(Float)
    clouds = Column(String(50))

class Test_Website(Base,Website_Data):
    __tablename__ = 'testwebsite'

class Accuweathercom(Base,Website_Data):
    __tablename__ ='accuweathercom'

class Openweathermaporg(Base,Website_Data):
    __tablename__ ='openweathermaporg'

class Wettercom(Base,Website_Data):
    __tablename__ ='wettercom'

class Wetterde(Base,Website_Data):
    __tablename__ ='wetterde'

class Wetterdienstde(Base,Website_Data):
    __tablename__ ='wetterdienstde'


#https://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
class Dwd(Base):
    __tablename__ ='dwd'

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
    #Load_Data('C:\\Users\Vika\Documents\TU\ProgPraktikum\data.csv', engine,se)
    bad_input = True
    while(bad_input):
        path = input("Enter a dircetory: ")
        websitename = input("Enter a table name: ")
        if(websitename not in all_table_names):
            print("Please enter a valid table name! (websites','testwebsite','accuweathercom','openweathermaporg','wettercom','wetterde','wetterdienstde)")
            continue
        try:
            Load_Data('C:\\Users\Vika\Documents\TU\ProgPraktikum\ssdasd.txt', se, "testwebsite")
        except:
            print("Please enter a valid directory!")
            continue
        bad_input = False

    #for date in se.query(Dwd.measure_date).filter(Dwd.average_temp < 15):
        #print(date)

    se.close()

def queryExample(test):

    print()
    #for istance in se.query(User).order


def printTables(m):
    for table in m.tables.values():
        print(table.name)
        for column in table.c:
            print(column.name)

def Load_Data(file_name,se,tablename):
    klassname = Dwd
    trennsymbol = ";"
    if tablename == "dwd":
        klassname = Dwd
    elif tablename == "accuweathercom":
        klassname = Accuweathercom
        trennsymbol = ","
    elif tablename == "wettercom":
        trennsymbol = ","
        klassname = Wettercom
    elif tablename == "testwebsite":
        trennsymbol = ","
        klassname = Test_Website

    Load_Data_real(file_name, se, trennsymbol, klassname)

def Load_Data_real(file_name,se,trennsymbol,klassname):

    datafile = open(file_name, 'r')
    readCSV = csv.reader(datafile, delimiter=trennsymbol)
    count = 0
    for row in readCSV:
        for i in range(len(row)):
            if row[i] == "nan" or row[i] == "None" or row[i] == "":
                row[i] = sqla.sql.null()
            elif klassname != Dwd:
                print(":)")
                #row[i] = row[i][1:-1]
        if count > 0:
            if klassname == Dwd:
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

            else:
                if count == 1:
                    wrow = Websites(**{
                        'websitename':row[0],
                        'url':row[0]
                    })
                    se.add(wrow)

                    try:
                        se.commit()
                    except sqla.exc.IntegrityError:
                        # se.expunge(row)
                        se.rollback()
                        print("°J°")
                date,hour = unix_time_to_normal_time(row[1])
                date_p,hour_p =unix_time_to_normal_time(row[2])
                nrow = klassname(**{
                    'measure_date' :date,
                    'measure_date_hour': hour,
                    'measure_date_prediction' :date_p,
                    'measure_date_prediction_hour': hour_p,
                    'postcode':row[3],
                    'city':row[4],
                    'temp':row[5],
                    'humidity_prob':row[6],
                    'precipitation_amount':row[7],
                    'precipitation_type':row[8],
                    'wind_speed':row[9],
                    'air_pressure_ground':row[10],
                    'air_pressure_sea':row[11],
                    'max_temp':row[12],
                    'min_temp':row[13],
                    'sun_hours':row[14],
                    'clouds':row[15]
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

def unix_time_to_normal_time(utime):
    stamp = int(float(utime))
    date = datetime.datetime.fromtimestamp(stamp).strftime('%Y%m%d')
    hour = datetime.datetime.fromtimestamp(stamp).strftime('%H')
    return date,hour

if __name__ == "__main__":
    main()


import datetime
import sys
import csv

import pymysql
import sqlalchemy as sqla
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *


DUMMY_POSTCODE = -1
DUMMY_CITY = "Whiterun"

upload_block_size = 1
stop_on_error = True

path_to_login_file = "Login/login_data"

newDB = True
dbname = 'pyWetter'
Base = declarative_base()
all_table_names = {'websites','testwebsite','accuweathercom','openweathermaporg','wettercom','wetterde','wetterdienstde','dwd','city_to_postcode'}

class City_to_postcode(Base):
    __tablename__ ='city_to_postcode'
    postcode = Column(Integer, primary_key=True)
    city = Column(String(50),primary_key=True)

class Websites(Base):
    __tablename__ = 'websites'
    websitename = Column(String(50), primary_key=True)
    url = Column(String(100))

class Website_Data():
    measure_date = Column(Integer,primary_key=True) #Wann die Vorgersage ausgelesen wurde
    measure_date_hour = Column(Integer)
    measure_date_prediction = Column(Integer, primary_key=True)  # Für wann die Vorhersage ist
    measure_date_prediction_hour = Column(Integer, primary_key=True)
    postcode = Column(Integer, primary_key=True)
    city = Column(String(50), primary_key=True)
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
    username, password = read_username_password(path_to_login_file)
    engine = create_engine('mysql+pymysql://'+username+':'+password+'@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')

    Base.metadata.create_all(engine)
    Session = sqla.orm.sessionmaker()
    Session.configure(bind=engine)
    se = Session()

    global stop_on_error
    global upload_block_size
    websitename = null;

    if (len(sys.argv)>1):
        if (sys.argv[1] not in all_table_names):
            print("Please enter a valid table name! (websites','testwebsite','accuweathercom','openweathermaporg','wettercom','wetterde','wetterdienstde)")
            sys.exit()
        if len(sys.argv)>3:
            if sys.argv[3] == '-all':
                stop_on_error = False
            else:
                upload_block_size = int(sys.argv[3])
        if len(sys.argv) > 4:
            if sys.argv[4] == '-all':
                stop_on_error = False
            else:
                upload_block_size = int(sys.argv[4])
        try:
            Load_Data(sys.argv[2], se, sys.argv[1])
        except Exception as e:
            print(e)
            return 1

    else:
        print("ERROR: too few arguments \n example: python Load_file.py <tablename> <filepath>")

    se.close()

def read_username_password(path_to_login_file):
    r"""
    Reads the username and password from the given path
    :param path_to_login_file: file path to the login data
    :return: username and password, given in the file
    """
    file = open(path_to_login_file, 'r')
    username = (file.readline()[10:-1])
    password = file.readline()[10:]
    file.close()
    return username,password


def run(tablename,file_name):
    r"""
    starts Load_Data
    :param tablename: the table to upload into
    :param file_name: the file to load from
    :return: 0 = True ; 1 = False
    """
    username, password = read_username_password(path_to_login_file)
    engine = create_engine('mysql+pymysql://'+username+':'+password+'@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')

    Base.metadata.create_all(engine)
    Session = sqla.orm.sessionmaker()
    Session.configure(bind=engine)
    se = Session()

    try:
        return Load_Data(file_name, se, tablename)
    except Exception as e:
        print(e)
        return 1
    return 0

def Load_Data(file_name,se,tablename):
    r"""
    loads a preset of parameters and starts uploading data (using Load_Data_real)
    :param file_name: the csv-file to upload
    :param se: the session to work on
    :param tablename: the table to upload into
    :return: void
    """
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
    elif tablename == 'openweathermaporg':
        klassname = Openweathermaporg
        trennsymbol = ","
    elif tablename == 'wetterdienstde':
        klassname = Wetterdienstde
        trennsymbol = ","
    elif tablename == 'city_to_postcode':
        klassname = City_to_postcode
        trennsymbol = ","
    elif tablename == 'wetterde':
        klassname = Wetterde
        trennsymbol = ","

    Load_Data_real(file_name, se, trennsymbol, klassname)

def Load_Data_real(file_name,se,trennsymbol,klassname):
    r"""
    uploads the csv-file into the given table on the server
    :param file_name: the csv-file to upload
    :param se: the session to work on
    :param trennsymbol: delimiter of the csv-file
    :param klassname: the name of the website-class
    :return: 0 = True ; 1 = False
    """
    datafile = open(file_name, 'r')
    readCSV = csv.reader(datafile, delimiter=trennsymbol)
    count = 0
    for row in readCSV:
        for i in range(len(row)):

            if row[i] == "nan" or row[i] == "None" or row[i] == "":
                if i == 3:
                    row[i] = DUMMY_POSTCODE
                elif i == 4:
                    row[i] = DUMMY_CITY
                else:
                    row[i] = sqla.sql.null()
        if count > 0:
            if (klassname == City_to_postcode):
                print(count)
                nrow = klassname(**{'postcode': row[0],
                                    'city' : row[1]})
                se.add(nrow)

            elif klassname == Dwd:
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
                if len(row)<1:
                    count = count +1
                    continue
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

                date,hour = unix_time_to_normal_time(row[1])
                date_p,hour_p = unix_time_to_normal_time(row[2])

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

            if (count % upload_block_size == 0):
                try:
                    se.commit()
                except Exception as e:
                    se.rollback()
                    print("Eintrag existiert schon:")
                    print(row)
                    print(e)
                    if( stop_on_error):
                        return 1

        count = count + 1
    try:
        se.commit()
    except Exception as e:
        se.rollback()
        print("Eintrag existiert schon:")
        print(row)
        print(e)
        if (stop_on_error):
            return 1

    return 0

def unix_time_to_normal_time(utime):
    r"""
    converts a unix-timestamp into the YYYY/MM/DD format and roughly the time of day in hours
    :param utime: the unix-timestamp to convert
    :return: the date as YYYYMMDD and time of day as HH
    """
    stamp = int(float(utime))
    date = datetime.datetime.fromtimestamp(stamp).strftime('%Y%m%d')
    hour = datetime.datetime.fromtimestamp(stamp).strftime('%H')
    return date,hour

if __name__ == "__main__":
    main()
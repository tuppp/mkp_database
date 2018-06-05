import numpy as np
import sqlalchemy as sqla
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

# Functions

def getPostcodeFromTable(postcode, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode):
        result.append(a)
    return np.vstack(result)


# rainy days
def getRain(postcode, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.precipitation_type != "kein NS"):
        result.append(a)
    return np.vstack(result)


# sunny days (x sun hours)
def getSunHours(postcode, sunHours, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.sun_hours >= sunHours):
        result.append(a)
    return np.vstack(result)


# sunny days (x sun hours) by date
def getSunHoursByDate(date, sunHours, table, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date).filter(table.c.sun_hours >= sunHours):
        result.append(a)
    return np.vstack(result)


# hot days (x tempavg)
def getTempAvg(postcode, avgTemp, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.average_temp >= avgTemp):
        result.append(a)
    return np.vstack(result)

# hot days (x tempavg)
def getTempAvgByDate(date, avgTemp, table, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date).filter(table.c.average_temp >= avgTemp):
        result.append(a)
    return np.vstack(result)


# a lot of rain
def getPrecAvg(postcode, avgPrec, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.precipitation_amount >= avgPrec):
        result.append(a)
    return np.vstack(result)


# look for precipitation type
def getPrecType(postcode, precipitationType, table):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(
            table.c.precipitation_type == precipitationType):
        result.append(a)
    return np.vstack(result)


# look for date
def getDate(date, table, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date):
        result.append(a)
    return np.vstack(result)


# look for date and postcode
def getDateAndPostcode(postcode, date, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.measure_date == date):
        result.append(a)
    return np.vstack(result)


# look for max wind
def getMaxWind(postcode, maxWind, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.max_wind_speed >= maxWind):
        result.append(a)
    return np.vstack(result)


# look for snow
def getSnowHeight(postcode, snow, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.snow_height >= snow):
        result.append(a)
    return np.vstack(result)


# look for avg wind speed
def getWindSpeedAvg(postcode, avgWindSpeed, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.average_wind_speed >= avgWindSpeed):
        result.append(a)
    return np.vstack(result)


# look for max temp
def getMaxTemp(postcode, maxTemp, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.max_temp >= maxTemp):
        result.append(a)
    return np.vstack(result)


# look for min # temp
def getMinTemp(postcode, minTemp, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.min_temp >= minTemp):
        result.append(a)
    return np.vstack(result)


# look for coverage
def getCoverage(postcode, coverage, table, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.coverage_amount >= coverage):
        result.append(a)
    return np.vstack(result)


####################################################

def main():
    try:
        Base = declarative_base()
        engine = create_engine('mysql+pymysql://dwdtestuser:asdassaj14123@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')
        # mysql --ssl-cipher=AES128-SHA -u dwdtestuser -p -h weather.service.tu-berlin.de dwdtest
        # mysql --ssl -u dwdtestuser -p -h weather.service.tu-berlin.de dwdtest
        # password asdassaj14123
        # use dwdtest
        Base.metadata.create_all(engine)
        Session = sqla.orm.sessionmaker()
        Session.configure(bind=engine)
        se = Session()
        # connect to database
        metadata = MetaData(engine, reflect=True)
    except Exception as error:
        if "Engine not found" in str(error):
            print("Engine not found")
        else:
            print(error)

    # Get Table
    Dwd = metadata.tables['dwd']

    # call functions
    result = getPostcodeFromTable(26197, Dwd, se)
    print(result)
    print()

    print("Tage mit Niederschlag")
    print(getRain(26197, Dwd, se))
    print()

    print("Tage mit mindestens x Sonnenstunden")
    print(getSunHours(26197, 12, Dwd, se))
    print()

    print("Orte mit mindestens x Sonnenstunden an bestimmten Tag")
    print(getSunHoursByDate(20171014, 5, Dwd, se))

    print("Tage mit mindestens x Grad Durschnittstemperatur")
    print(getTempAvg(26197, 20, Dwd, se))
    print()

    print("Alle Ort mit mindestens x Grad Durschnittstemperatur an bestimmten Tag")
    print(getTempAvgByDate(20170410, 5, Dwd, se))
    print()

    print("Tage mit mindestens x Niederschlagsmenge")
    print(getPrecAvg(26197, 1.0, Dwd, se))
    print()

    print("Nach Datum suchen")
    print(getDate(20171013, Dwd, se))
    print()

    print("Nach Datum und Postcode suchen")
    print(getDateAndPostcode(26197, 20170512, Dwd, se))
    print()

    print("Nach snowHeight suchen")
    print(getSnowHeight(26197, 0.1, Dwd, se))
    print()

    print("Nach maxTemp suchen")
    print(getMaxTemp(26197, 25.0, Dwd, se))
    print()

    print("Nach minTemp suchen")
    print(getMinTemp(26197, 5.0, Dwd, se))
    print()

    se.close()


if __name__ == '__main__':
    main()
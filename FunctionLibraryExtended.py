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
def getSunHours(postcode, table, sunHours, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.sun_hours >= sunHours):
        result.append(a)
    return np.vstack(result)


# sunny days (x sun hours) by date
def getSunHoursByDate(date, table, sunHours, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date).filter(table.c.sun_hours >= sunHours):
        result.append(a)
    return np.vstack(result)


# hot days (x tempavg)
def getTempAvg(postcode, table, avgTemp, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.average_temp >= avgTemp):
        result.append(a)
    return np.vstack(result)


# hot days (x tempavg)
def getTempAvgByDate(date, table, avgTemp, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date).filter(table.c.average_temp >= avgTemp):
        result.append(a)
    return np.vstack(result)


# a lot of rain
def getPrecAvg(postcode, table, avgPrec, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.precipitation_amount >= avgPrec):
        result.append(a)
    return np.vstack(result)


# look for precipitation type
def getPrecType(postcode, table, precipitationType):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(
            table.c.precipitation_type == precipitationType):
        result.append(a)
    return np.vstack(result)


# look for date
def getDate(table, date, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date):
        result.append(a)
    return np.vstack(result)


# look for date and postcode
def getDateAndPostcode(postcode, table, date, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.measure_date == date):
        result.append(a)
    return np.vstack(result)


# look for max wind
def getMaxWind(postcode, table, maxWind, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.max_wind_speed >= maxWind):
        result.append(a)
    return np.vstack(result)


# look for snow
def getSnowHeight(postcode, table, snow, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.snow_height >= snow):
        result.append(a)
    return np.vstack(result)


# look for avg wind speed
def getWindSpeedAvg(postcode, table, avgWindSpeed, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.average_wind_speed >= avgWindSpeed):
        result.append(a)
    return np.vstack(result)


# look for max temp
def getMaxTemp(postcode, table, maxTemp, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.max_temp >= maxTemp):
        result.append(a)
    return np.vstack(result)


# look for min # temp
def getMinTemp(postcode, table, minTemp, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.min_temp >= minTemp):
        result.append(a)
    return np.vstack(result)


# look for coverage
def getCoverage(postcode, table, coverage, se):
    result = []
    for a in se.query(table).filter(table.c.postcode == postcode).filter(table.c.coverage_amount >= coverage):
        result.append(a)
    return np.vstack(result)


####################################################

Base = declarative_base()
engine = create_engine(
    'mysql+pymysql://dwdtestuser:asdassaj14123@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')
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

# Get Table
Dwd = metadata.tables['dwd']

# call functions
result = getPostcodeFromTable(26197, Dwd, se)
print(result)
print()

print("Tage mit Niederschlag: ")
print(getRain(26197, Dwd, se))
print()

print("Tage mit mindestens x Sonnenstunden: ")
print(getSunHours(26197, Dwd, 12, se))
print()

print("Tage mit mindestens x Grad Durschnittstemperatur")
print(getTempAvg(26197, Dwd, 20, se))
print()

print("Tage mit mindestens x Grad Durschnittstemperatur")
print(getTempAvg(26197, Dwd, 20, se))
print()

print("Alle Ort mit mindestens x Grad Durschnittstemperatur an bestimmten Tag")
print(getTempAvgByDate(20170410, Dwd, 5, se))
print()

print("Tage mit mindestens x Niederschlagsmenge")
print(getPrecAvg(26197, Dwd, 1.0, se))
print()

print("Nach Datum suchen")
print(getDate(Dwd, 20171013, se))
print()

print("Nach snowHeight suchen")
print(getSnowHeight(26197, Dwd, 0.1, se))
print()

print("Nach maxTemp suchen")
print(getMaxTemp(26197, Dwd, 25.0, se))
print()

print("Nach minTemp suchen")
print(getMinTemp(26197, Dwd, 5.0, se))
print()

print("Nach Datum und Postcode suchen")
print(getDateAndPostcode(26197, Dwd, 20170512, se))
print()


se.close()

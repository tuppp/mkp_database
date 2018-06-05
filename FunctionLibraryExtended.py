import numpy as np
import sqlalchemy as sqla
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

<<<<<<< HEAD
# auxiliary functions

# convert a list of column name strings into parameters to call, if no columns given: return the whole table
def getColumnList(columnlist, table, se):
    if not columnlist:
        return table
    parameters = []
    for col in columnlist:
        parameters.append(getattr(table.c, col))
    return parameters

=======
>>>>>>> 2cc02420f8e9ec5b16dab0f1034ce92db9f05124

# Functions

def getPostcodeFromTable(postcode, table, se, query=None):
    if (query == None):
        query = se.query(table).filter(table.c.postcode == postcode)
        return query
    else:
        query = query.filter(table.c.postcode == postcode)
        return query


# rainy days
def getRain(table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.precipitation_type != "kein NS")
        return query
    else:
        query = query.filter(table.c.precipitation_type != "kein NS")
        return query

# sunny days (x sun hours)
<<<<<<< HEAD
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
=======
def getSunHours(sunHours, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.sun_hours == sunHours)
        return query
    else:
        query = query.filter(table.c.sun_hours == sunHours)
        return query

# sunny days (x sun hours) by date
def getSunHoursByDate(date, sunHours, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.measure_date == date).filter(table.c.sun_hours == sunHours)
        return query
    else:
        query = query.filter(table.c.measure_date == date).filter(table.c.sun_hours == sunHours)
        return query

# hot days (x tempavg)
def getTempAvg(avgTemp, table, se, query=None):
    if (query == None):
        query = se.query(table).filter(table.c.average_temp == avgTemp)
        return query
    else:
        query = query.filter(table.c.average_temp == avgTemp)
        return query

>>>>>>> 2cc02420f8e9ec5b16dab0f1034ce92db9f05124


# hot days (x tempavg)
def getTempAvgByDate(date, table, avgTemp, se):
    result = []
    for a in se.query(table).filter(table.c.measure_date == date).filter(table.c.average_temp >= avgTemp):
        result.append(a)
    return np.vstack(result)

##########################################

# a lot of rain
def getMorePercipitation(columlist, postcode, number, table, se):
    result = []
    for a in se.query(*getColumnList(columlist, table, se)).filter(table.c.postcode == postcode).filter(table.c.precipitation_amount >= avgPrec):
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
<<<<<<< HEAD
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
##########################################

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
=======
def getDate(date, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.measure_date == date)
        return query
    else:
        query = query.filter(table.c.measure_date == date)
        return query


# look for date and postcode
def getDateAndPostcode(postcode, date, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.postcode == postcode).filter(table.c.measure_date == date)
        return query
    else:
        query = query.filter(table.c.postcode == postcode).filter(table.c.measure_date == date)
        return query


# look for max wind
def getMaxWind(postcode, maxWind, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.postcode == postcode).filter(table.c.max_wind_speed >= maxWind)
        return query
    else:
        query = query.filter(table.c.postcode == postcode).filter(table.c.max_wind_speed >= maxWind)
        return query


# look for snow
def getSnowHeight(postcode, snow, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.postcode == postcode).filter(table.c.snow_height >= snow)
        return query
    else:
        query = query.filter(table.c.postcode == postcode).filter(table.c.snow_height >= snow)
        return query


# look for avg wind speed
def getWindSpeedAvgUp(avgWindSpeed, table, se, query):
    if(query == None):
        query = se.query(table).filter(table.c.average_wind_speed >= avgWindSpeed)
        return query
    else:
        query = query.filter(table.c.average_wind_speed >= avgWindSpeed)
        return query


def getWindSpeedAvgDown(avgWindSpeed, table, se, query):
    if(query == None):
        query = se.query(table).filter(table.c.average_wind_speed <= avgWindSpeed)
        return query
    else:
        query = query.filter(table.c.average_wind_speed <= avgWindSpeed)
        return query


# look for max temp
def getMaxTempUp(maxTemp, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.max_temp >= maxTemp)
        return query
    else:
        query = query.filter(table.c.max_temp >= maxTemp)
        return query


def getMaxTempDown(maxTemp, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.max_temp <= maxTemp)
        return query
    else:
        query = query.filter(table.c.max_temp <= maxTemp)
        return query


# look for min # temp
def getMinTempUp(minTemp, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.min_temp >= minTemp)
        return query
    else:
        query = query.filter(table.c.min_temp >= minTemp)
        return query

def getMinTempDown(minTemp, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.min_temp <= minTemp)
        return query
    else:
        query = query.filter(table.c.min_temp <= minTemp)
        return query


# look for coverage
def getCoverageUp(coverage, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.coverage_amount >= coverage)
        return query
    else:
        query = query.filter(table.c.coverage_amount >= coverage)
        return query


def getCoverageDown(coverage, table, se, query=None):
    if(query == None):
        query = se.query(table).filter(table.c.coverage_amount <= coverage)
        return query
    else:
        query = query.filter(table.c.coverage_amount <= coverage)
        return query


def getResult(query):
>>>>>>> 2cc02420f8e9ec5b16dab0f1034ce92db9f05124
    result = []
    for a in query:
        result.append(a)
    if result == []:
        return "Result was empty"
    else:
        return np.vstack(result)

####################################################

<<<<<<< HEAD
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

#Test functions

print("Tage mit mindestens x Niederschlagsmenge")
print(getMorePercipitation(["station_id", "station_name", "postcode", "precipitation_amount"], 26197, 1.0, Dwd, se))
print()

print("Nach Datum suchen")
print(getDate(Dwd, 20171013, se))
print()

# print("Nach snowHeight suchen")
# print(getSnowHeight(26197, Dwd, 0.1, se))
# print()

# print("Nach maxTemp suchen")
# print(getMaxTemp(26197, Dwd, 25.0, se))
# print()

# print("Nach minTemp suchen")
# print(getMinTemp(26197, Dwd, 5.0, se))
# print()

# print("Nach Datum und Postcode suchen")
# print(getDateAndPostcode(26197, Dwd, 20170512, se))
# print()


se.close()
=======
def main():
    try:
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
    except Exception as error:
        if "Engine not found" in str(error):
            print("Engine not found")
        else:
            print(error)

    # Get Table
    Dwd = metadata.tables['dwd']

    # # Test new functions
    print("Filter auf Postleitzahl danach auf avg_temp")
    print(getResult(getTempAvg(0, Dwd, se, getPostcodeFromTable(26197, Dwd, se))))

    print()
    print("Postcode")
    print(getResult(getPostcodeFromTable(26197,Dwd,se)))

    print()
    print("Rain")
    print(getResult(getRain(Dwd,se)))

    print()
    print("SunHours")
    print(getResult(getSunHours(12,Dwd,se)))

    print()
    print("SunHoursByDate")
    print(getResult(getSunHoursByDate(20170410,12,Dwd,se)))

    print()
    print("TempAvg")
    print(getResult(getTempAvg(22,Dwd,se)))

    print()
    print("Postcode on Rain")
    print(getResult(getRain(Dwd,se,getPostcodeFromTable(26197,Dwd,se))))

    print()
    print("Postcode on SunHours on tempAvg")
    print(getResult(getTempAvg(22,Dwd,se,getSunHours(12,Dwd,se,getPostcodeFromTable(26197,Dwd,se)))))

    se.close()



if __name__ == '__main__':
    main()
>>>>>>> 2cc02420f8e9ec5b16dab0f1034ce92db9f05124

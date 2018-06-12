import numpy as np
import sqlalchemy as sqla
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base


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


def getResult(query):
    result = []
    for a in query:
        result.append(a)
    if result == []:
        return "Result was empty"
    else:
        return np.vstack(result)

####################################################

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

    # Test new functions
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

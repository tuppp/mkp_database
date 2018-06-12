import numpy as np
import sqlalchemy as sqla
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base


# Functions

def getPostcode(postcode, table, se, query=None):
    r"""
    Method gets all entries with given postcode
    :param postcode: desired postcode
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.postcode == postcode)
        return query
    else:
        query = query.filter(table.c.postcode == postcode)
        return query


# rainy days
def getRain(table, se, query=None):
    r"""
        Returns rain information
       :param table: which weather table is going to be used
       :param se: Session Object containing connection information
       :param query: Query Object which contains SQL query, if empty one will be created
       :returns: Query Object, can be reused for other queries
       """
    if (query == None):
        query = se.query(table).filter(table.c.precipitation_type != "kein NS")
        return query
    else:
        query = query.filter(table.c.precipitation_type != "kein NS")
        return query


# sunny days (x sun hours)
def getSunHours(sunHours, table, se, query=None):
    r"""
       Method gets all entries which matching amount of sun
       :param sunHours: desired sunhours
       :param table: which weather table is going to be used
       :param se: Session Object containing connection information
       :param query: Query Object which contains SQL query, if empty one will be created
       :returns: Query Object, can be reused for other queries
       """
    if (query == None):
        query = se.query(table).filter(table.c.sun_hours == sunHours)
        return query
    else:
        query = query.filter(table.c.sun_hours == sunHours)
        return query

# hot days (x tempavg)
def getTempAvg(avgTemp, table, se, query=None):
    r"""
       Method gets all entries which desired average temperature
       :param avgTemp: desired average temperature
       :param table: which weather table is going to be used
       :param se: Session Object containing connection information
       :param query: Query Object which contains SQL query, if empty one will be created
       :returns: Query Object, can be reused for other queries
       """
    if (query == None):
        query = se.query(table).filter(table.c.average_temp == avgTemp)
        return query
    else:
        query = query.filter(table.c.average_temp == avgTemp)
        return query

# a lot of rain
def getPrecAvg(avgPrec, table, se, query=None):
    r"""
       Method gets all entries with average precipitation higher than/equal to input average precipitation
       :param avgPrec: desired average temperature
       :param table: which weather table is going to be used
       :param se: Session Object containing connection information
       :param query: Query Object which contains SQL query, if empty one will be created
       :returns: Query Object, can be reused for other queries
       """
    if (query == None):
        query = se.query(table).filter(table.c.precipitation_amount >= avgPrec)
        return query
    else:
        query = query.filter(table.c.precipitation_amount >= avgPrec)
        return query


# look for precipitation type
def getPrecType(precipitationType, table, se, query=None):
    r"""
       Method gets all entries with given pecipitation type
       :param precipitationType: desired pecipitation type
       :param table: which weather table is going to be used
       :param se: Session Object containing connection information
       :param query: Query Object which contains SQL query, if empty one will be created
       :returns: Query Object, can be reused for other queries
       """
    if (query == None):
        query = se.query(table).filter(table.c.precipitation_type == precipitationType)
        return query
    else:
        query = query.filter(table.c.precipitation_type == precipitationType)
        return query


# look for date
def getDate(date, table, se, query=None):
    r"""
       Method gets all entries with given date
       :param postcode: date YYYYMMDD (e.g. 20170512)c
       :param table: which weather table is going to be used
       :param se: Session Object containing connection information
       :param query: Query Object which contains SQL query, if empty one will be created
       :returns: Query Object, can be reused for other queries
       """
    if query is None:
        query = se.query(table).filter(table.c.measure_date == date)
        return query
    else:
        query = query.filter(table.c.measure_date == date)
        return query

# look for max wind >=
def getMaxWindUp(maxWind, table, se, query=None):
    r"""
    get all data from table with max_wind_speed >= given value
    :param maxWind: max_wind_speed value
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if query is None:
        query = se.query(table).filter(table.c.max_wind_speed >= maxWind)
        return query
    else:
        query = query.filter(table.c.max_wind_speed >= maxWind)
        return query

# look for snow height >=
def getSnowHeightUp(snow, table, se, query=None):
    r"""
    get all data from table with snow_height >= given value
    :param snow: snow_height value
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if query is None:
        query = se.query(table).filter(table.c.snow_height >= snow)
        return query
    else:
        query = query.filter(table.c.snow_height >= snow)
        return query

# look for avg wind speed >=
def getWindSpeedAvgUp(avgWindSpeed, table, se, query):
    r"""
    get all data from table with average_wind_speed >= given value
    :param avgWindSpeed: average_wind_speed value
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.average_wind_speed >= avgWindSpeed)
        return query
    else:
        query = query.filter(table.c.average_wind_speed >= avgWindSpeed)
        return query

def getWindSpeedAvgDown(avgWindSpeed, table, se, query):
    r"""
    get all data from table with average_wind_speed <= given value
    :param avgWindSpeed: average_wind_speed value
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.average_wind_speed <= avgWindSpeed)
        return query
    else:
        query = query.filter(table.c.average_wind_speed <= avgWindSpeed)
        return query

# look for max temp
def getMaxTempUp(maxTemp, table, se, query=None):
    r"""
    get all data from table with max_temp >= given value
    :param maxTemp: max_temp value
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.max_temp >= maxTemp)
        return query
    else:
        query = query.filter(table.c.max_temp >= maxTemp)
        return query

def getMaxTempDown(maxTemp, table, se, query=None):
    r"""
    get all data from table with max_temp <= given value
    :param maxTemp: max_temp value
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :returns: Query Object, can be reused for other queries
    """
    if(query == None):
        query = se.query(table).filter(table.c.max_temp <= maxTemp)
        return query
    else:
        query = query.filter(table.c.max_temp <= maxTemp)
        return query

# look for min # temp
def getMinTempUp(minTemp, table, se, query=None):
    r"""
    Returns table Data for the given Temperature upwards
    :param minTemp: Temperature to sort downwards (>=)
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :return: Query Object, can be reused for other queries
    """
    if(query == None):
        query = se.query(table).filter(table.c.min_temp >= minTemp)
        return query
    else:
        query = query.filter(table.c.min_temp >= minTemp)
        return query

def getMinTempDown(minTemp, table, se, query=None):
    r"""
    Returns table Data for the given Temperature downwards

    :param minTemp: Temperature to sort downwards (<=)
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :return: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.min_temp <= minTemp)
        return query
    else:
        query = query.filter(table.c.min_temp <= minTemp)
        return query

# look for coverage
def getCoverageUp(coverage, table, se, query=None):
    r"""
    Returns table Data for the given Coverage upwards

    :param coverage: Coverage to sort upwards (>=)
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :return: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.coverage_amount >= coverage)
        return query
    else:
        query = query.filter(table.c.coverage_amount >= coverage)
        return query

def getCoverageDown(coverage, table, se, query=None):
    r"""
    Returns table Data for the given Coverage downwards

    :param coverage: Coverage to sort downwards (<=)
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :param query: Query Object which contains SQL query, if empty one will be created
    :return: Query Object, can be reused for other queries
    """
    if (query == None):
        query = se.query(table).filter(table.c.coverage_amount <= coverage)
        return query
    else:
        query = query.filter(table.c.coverage_amount <= coverage)

        return query


# convert a list of column name strings into parameters to call, if no columns given: return the whole table
# Eg: se.query(*getColumnList(columlist, table, se)).filter(table.c.postcode == postcode)
def getColumnList(columnlist, table, se):
    r"""
    if columnlist is not specified: query on the whole table,
    else: only display given columns
    <!> Must be called before calling other queries

    :param columnlist:  list of column names that should be displayed, eg: ["station_id", "station_name", "postcode", "precipitation_amount"]
    :param table: which weather table is going to be used
    :param se: Session Object containing connection information
    :returns: Query Object, can be reused for other queries
    """
    if not columnlist:
        return se.query(table)
    parameters = []
    for col in columnlist:
        parameters.append(getattr(table.c, col))
    return se.query(*parameters)


# Finish query and return
########################
# Session close hier!?###
########################
def getResult(query, se):
    r"""
    :param query: Query Object which contains SQL query
    :param se: Session Object containing the connection information
    :returns: NumpyArray containing all information
    """
    result = []
    for a in query:
        result.append(a)
    if result == []:
        se.close()
        return "Result was empty"
    else:
        se.close()
        return np.vstack(result)

#Setup Data
def getConnectionData(tablename):
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
    table = metadata.tables[tablename]

    return (table, se)

def getConnectionDWD():
    return getConnectionData('dwd')

def getConnectionWetterdienstde():
    return getConnectionData('wetterdienstde')

def getConnectionWetterde():
    return getConnectionData('wetterde')

def getConnectionWettercom():
    return getConnectionData('wettercom')

def getConnectionOpenWeatherMaporg():
    return getConnectionData('openweathermaporg')

def getConnectionAccuweathercom():
    return getConnectionData('accuweathercom')

def getConnectionTest():
    return getConnectionData('testwebsite')

####################################################

def main():
    r"""
    TESTFUNCTION DO NOT USE
    """
    Dwd, se = getConnectionAccuweathercom()

    # # Test new functions
    print("Filter auf Postleitzahl danach auf avg_temp")
    print(getResult(getTempAvg(0, Dwd, se, getPostcode(26197, Dwd, se)), se))

    print()
    print("Postcode")
    print(getResult(getPostcode(26197, Dwd, se), se))

    print()
    print("Rain")
    print(getResult(getRain(Dwd, se), se))

    print()
    print("SunHours")
    print(getResult(getSunHours(12, Dwd, se), se))

    print()
    print("TempAvg")
    print(getResult(getTempAvg(22, Dwd, se), se))

    print()
    print("Postcode on Rain")
    print(getResult(getRain(Dwd, se, getPostcode(26197, Dwd, se)), se))

    print()
    print("Postcode on SunHours on tempAvg")
    print(getResult(getTempAvg(22, Dwd, se, getSunHours(12, Dwd, se, getPostcode(26197, Dwd, se))), se))

if __name__ == '__main__':
    main()
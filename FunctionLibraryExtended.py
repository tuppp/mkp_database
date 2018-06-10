import numpy as np
import sqlalchemy as sqla
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base


# Functions

def getPostcode(postcode, table, se, query=None):
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

# hot days (x tempavg)
def getTempAvg(avgTemp, table, se, query=None):
    if (query == None):
        query = se.query(table).filter(table.c.average_temp == avgTemp)
        return query
    else:
        query = query.filter(table.c.average_temp == avgTemp)
        return query

# a lot of rain
def getPrecAvg(avgPrec, table, se, query= None):
    if (query == None):
        query = se.query(table).filter(table.c.precipitation_amount >= avgPrec)
        return query
    else:
        query = query.filter(table.c.precipitation_amount >= avgPrec)
        return query


# look for precipitation type
def getPrecType(precipitationType, table, se, query=None):
    if (query == None):
        query = se.query(table).filter(table.c.precipitation_type == precipitationType)
        return query
    else:
        query = query.filter(table.c.precipitation_type == precipitationType)
        return query


# look for date
def getDate(date, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.measure_date == date)
        return query
    else:
        query = query.filter(table.c.measure_date == date)
        return query

# look for max wind
def getMaxWind(maxWind, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.max_wind_speed >= maxWind)
        return query
    else:
        query = query.filter(table.c.max_wind_speed >= maxWind)
        return query

# look for snow
def getSnowHeight(snow, table, se, query=None):
    if query is None:
        query = se.query(table).filter(table.c.snow_height >= snow)
        return query
    else:
        query = query.filter(table.c.snow_height >= snow)
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

#Finish query and return
########################
#Session close hier!?###
########################

def getResult(query,se):
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
def getConnectionData(UseDwd = bool):
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
    if(UseDwd):
        Dwd = metadata.tables['dwd']
    else:
        Dwd = metadata.tables['web']
    return (Dwd,se)

####################################################

def main():
    Dwd,se = getConnectionData(true)

    # # Test new functions
    print("Filter auf Postleitzahl danach auf avg_temp")
    print(getResult(getTempAvg(0, Dwd, se, getPostcode(26197, Dwd, se)),se))

    print()
    print("Postcode")
    print(getResult(getPostcode(26197,Dwd,se),se))

    print()
    print("Rain")
    print(getResult(getRain(Dwd,se),se))

    print()
    print("SunHours")
    print(getResult(getSunHours(12,Dwd,se),se))

    print()
    print("TempAvg")
    print(getResult(getTempAvg(22,Dwd,se),se))

    print()
    print("Postcode on Rain")
    print(getResult(getRain(Dwd,se,getPostcode(26197,Dwd,se)),se))

    print()
    print("Postcode on SunHours on tempAvg")
    print(getResult(getTempAvg(22,Dwd,se,getSunHours(12,Dwd,se,getPostcode(26197,Dwd,se))),se))

if __name__ == '__main__':
    main()
import sqlalchemy as sqla
import pymysql
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *

# Functions

def getPostcodeFromTable(postcode, table, se):
	result = []
	for a in se.query(Dwd).filter(Dwd.c.postcode == postcode):
		result.append(a)
	return np.vstack(result)

####################################################

Base = declarative_base()
engine = create_engine('mysql+pymysql://dwdtestuser:asdassaj14123@weather.service.tu-berlin.de/dwdtest?use_unicode=1&charset=utf8&ssl_cipher=AES128-SHA')
    #mysql --ssl-cipher=AES128-SHA -u dwdtestuser -p -h weather.service.tu-berlin.de dwdtest
    #mysql --ssl -u dwdtestuser -p -h weather.service.tu-berlin.de dwdtest
	#password asdassaj14123
	#use dwdtest
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

se.close()

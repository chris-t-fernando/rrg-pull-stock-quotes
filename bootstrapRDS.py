# todo: run Selenium scrape to get sector:stock map, update RDS with it

import logging, sys
import boto3
import csv
import mysql.connector
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# handler for pulling config from SSM
def getSSMParameter(ssm, parameterPath, encryptionOption=False):
    return ssm.get_parameter(Name=parameterPath, WithDecryption=encryptionOption).get('Parameter').get('Value')

# set up boto SSM
ssm = boto3.client('ssm')

logging.info('Connecting to DB') 

# set up MySQL
mydb = mysql.connector.connect(
    host = getSSMParameter(ssm, '/rrg-creator/rds-endpoint'),
    user = getSSMParameter(ssm, '/rrg-creator/rds-user'),
    password = getSSMParameter(ssm, '/rrg-creator/rds-password', True),
    database = getSSMParameter(ssm, '/rrg-creator/rds-database')
)

logging.info('Connected to DB')

mycursor = mydb.cursor()

stockSql = 'INSERT INTO stock (stock_code, stock_name, sector_code) VALUES (%s, %s, %s)'

# use executemany since we're expecting the database to be empty anyway
# loop through bootstrap sectormap file to load stock codes
# ideally don't need to do this since we should really just scrape it straight in, but whatever
with open('sectormap.csv', newline='') as csvfile:
    mapReader = csv.DictReader(csvfile, delimiter=',')

    val = []

    for row in mapReader:
        val.append(tuple([row['ticker'].lower(), 'someName', row['sector'].lower()]))

if len(val) > 0:
    try:
        mycursor.executemany(stockSql, val)
        mydb.commit()
        print('Success:', mycursor.rowcount, 'stock records inserted.')
    except mysql.connector.errors.IntegrityError as e:
        print('Failed:', str(e))
else:
    print('No stock rows to insert')

#
#    quote_date DATE NOT NULL PRIMARY KEY,
#    sector_code VARCHAR(6) NOT NULL,
#    open_price FLOAT NOT NULL,
#    high_price FLOAT NOT NULL,
#    low_price FLOAT NOT NULL,
#    close_price FLOAT NOT NULL,
#    volume INT NOT NULL,
sectorQuoteSql = 'INSERT INTO weekly_sector_quotes (quote_date, sector_code, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'

# now load the sector quotes we have on hand
with open('sectorQuotes.csv', newline='') as csvfile:
    quoteReader = csv.DictReader(csvfile, delimiter=',')

    val = []
    
    for row in quoteReader:
        # volume needs to be expanded out from M and B to units
        if row['volume'][-1] == 'B':
            # billions
            volume = float(row['volume'].rstrip('B')) * 1000000000
        elif row['volume'][-1] == 'M':
            # millions
            volume = float(row['volume'].rstrip('M')) * 1000000
        elif row['volume'][-1] == 'K':
            # millions
            volume = float(row['volume'].rstrip('K')) * 1000
        else:
            volume = float(row['volume'])

        # CSV headers
        #   sectorticker,date,close,open,high,low,volume,pct-change
        # table columns
        #   quote_date, sector_code, open_price, high_price, low_price, close_price, volume
        val.append(
            tuple(
                [
                    row['date'],
                    row['sectorticker'].lower(),
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    volume
                ]
            )
        )

if len(val) > 0:
    try:
        mycursor.executemany(sectorQuoteSql, val)
        mydb.commit()
        print('Success:', mycursor.rowcount, 'sector quote records inserted.')
    except mysql.connector.errors.IntegrityError as e:
        print('Failed:', str(e))
else:
    print('No sector quote rows to insert')

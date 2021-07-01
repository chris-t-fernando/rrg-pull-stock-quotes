from os import close
import boto3
import logging, sys
import mysql.connector
import math
import yfinance as yf
from datetime import datetime
from datetime import timedelta
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# can only reference tuples by position index, not by column name - annoying!
# shamelessly stolen from https://kadler.io/2018/01/08/fetching-python-database-cursors-by-column-name.html#
class CursorByName():
    def __init__(self, cursor):
        self._cursor = cursor
    
    def __iter__(self):
        return self

    def __next__(self):
        row = self._cursor.__next__()
        return { description[0]: row[col] for col, description in enumerate(self._cursor.description) }

# handler for pulling config from SSM
def getSSMParameter(ssm, parameterPath, encryptionOption=False):
    return ssm.get_parameter(Name=parameterPath, WithDecryption=encryptionOption).get('Parameter').get('Value')

# there are nan's in the YF data :(
def cleanNaN(number):
    if math.isnan(number):
        return ''
    else:
        return number
    
def getQuotes(row):
    val = []
    try:
        # CursorByName gives me an dictionary with column names as keys
        
        if row['last_quote'] == None:
            # don't have quotes for this stock
            thisTicker = yf.Ticker(row['stock_code'] + '.AX').history(period='max', interval='1wk', actions=False)
            logging.debug('Stock %s: New ticker detected', row['stock_code'])

        else:
            # already have some quotes for this stock
            # increment row['last_quote'] by 1 day
            #lastQuoteObject = datetime.strptime(lastQuote, '%y-%m-%d')
            lastQuotePlusOne = row['last_quote'] + timedelta(days=7)
            logging.debug('Stock %s: Querying for date %s', row['stock_code'], lastQuotePlusOne)

            # if the current date == last_quote then YF throws an error because
            # the last_quote+1 is later than the finish date
            # eg. - 3PL.AX: Invalid input - start date cannot be after end date. startDate = 1624975200, endDate = 1624968592
            endDate = datetime.now() + timedelta(days=7)

            thisTicker = yf.Ticker(row['stock_code'] + '.AX').history(start=lastQuotePlusOne, end=endDate, interval='1wk', actions=False)
            logging.debug('Stock %s: Existing ticker detected. Last record timestamp is %s', row['stock_code'], str(row['last_quote']))
            
            # just a flag so I can include this detail in the output message, just before the return
            firstQuote = True
        
        # for each record returned by yf (one record = one week)
        for tickerRow in thisTicker.itertuples():
            # Index = yyyy-mm-dd 00:00:00
            # it would probably be better to interpret these as a timestamp and then format to just keep yyyy-mm-dd
            # ...but yolo
            thisDate = str(tickerRow.Index)[:10]
            val.append(
                tuple(
                    [
                        thisDate,
                        row['stock_code'],
                        cleanNaN(tickerRow.Open),
                        cleanNaN(tickerRow.High),
                        cleanNaN(tickerRow.Low),
                        cleanNaN(tickerRow.Close),
                        cleanNaN(tickerRow.Volume)
                    ]
                )
            )

        if firstQuote:
            logging.info('Stock %s: No quotes in database. Returning %s quotes', row['stock_code'], len(val))
        else:
            logging.info('Stock %s: Existing quotes in database. Returning %s quotes since %s', row['stock_code'], len(val), str(endDate))

        # return the result
        return val

    except Exception as e:
        # got a failure
        # hold on to the row that failed
        logging.error('Stock %s: YF failure.  Error: %s', row['stock_code'], str(Exception))
        
        # do we need to sleep too?  Ratelimited?

        return False


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

mycursor = mydb.cursor(buffered=True)

insertStatement = 'INSERT IGNORE INTO weekly_stock_quotes (quote_date, stock_code, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'

# get the stock codes
#mycursor.execute('SELECT * FROM stock ORDER BY sector_code')
mycursor.execute("""
select stock.stock_code, max(weekly_stock_quotes.quote_date) as last_quote
from stock
left join weekly_stock_quotes on stock.stock_code = weekly_stock_quotes.stock_code
group by stock.stock_code;
""")

logging.info('Got list of stock codes from DB')

# holds a List of Tuples that we'll use for a bulk insertmany of quotes for each company
# I could put all company quotes into a single val and call one insertmany, but maybe this is better for distributing risk/failure
val = []

# if there's a failure during the iteration of CursorByName, hold on to the stock symbol so we can retry later
retries = []

# now get the weekly quotes for each
for row in CursorByName(mycursor):
    quotes = getQuotes(row)

    if not quotes:
        retries.append(row)
    else:
        val = val + quotes

# try again for the failures - just once - if it keeps failing there's probably a bigger issue at play
for row in retries:
    quotes = getQuotes(row)

    if not quotes:
        retries.append(row)
    else:
        val = val + quotes

# check if there are any records to load in
if len(val) > 0:
    try:
        mycursor.executemany(insertStatement, val)
        logging.info('Stock %s: Inserted %s quotes', row['stock_code'], str(mycursor.rowcount))

    except mysql.connector.errors.IntegrityError as e:
        logging.error('Stock %s: Failed to insert records. Error dump: %s', row['stock_code'], str(e))
else:
    logging.info('Stock %s: No rows inserted', row['stock_code'])

mydb.commit()

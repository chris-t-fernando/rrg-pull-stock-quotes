from os import close
import boto3
import logging, sys
import mysql.connector
import math
import json
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

# there are NaN's in the YF data :(
def cleanNaN(number):
    if math.isnan(number):
        return ''
    else:
        return number

# set up boto SSM
ssmClient = boto3.client('ssm')

# get queue endpoint
queueUrl = getSSMParameter(ssmClient, '/rrg-creator/queue-endpoint')
queueClient = boto3.client('sqs')

logging.info('Connecting to DB') 

# set up MySQL
mydb = mysql.connector.connect(
    host = getSSMParameter(ssmClient, '/rrg-creator/rds-endpoint'),
    user = getSSMParameter(ssmClient, '/rrg-creator/rds-user'),
    password = getSSMParameter(ssmClient, '/rrg-creator/rds-password', True),
    database = getSSMParameter(ssmClient, '/rrg-creator/rds-database')
)

logging.info('Connected to DB')

mycursor = mydb.cursor(buffered=True)

#insertStatement = 'INSERT IGNORE INTO weekly_stock_quotes (quote_date, stock_code, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'

# get the stock codes
#mycursor.execute('SELECT * FROM stock ORDER BY sector_code')
mycursor.execute("""
select stock.stock_code, max(weekly_stock_quotes.quote_date) as last_quote
from stock
left join weekly_stock_quotes on stock.stock_code = weekly_stock_quotes.stock_code
group by stock.stock_code;
""")

logging.info('Got list of stock codes from DB')

# if there's a failure during the iteration of CursorByName, hold on to the stock symbol so we can retry later
retries = []

periodEnd = datetime.now()

# now get the weekly quotes for each
for row in CursorByName(mycursor):
    # holds all the values for this stock_code that will be sent in a single SQS message.  1 message per stock code
    val = []

    # CursorByName gives me an dictionary with column names as keys
    if row['last_quote'] == None:
        # don't have any quotes for this stock
        periodStart = datetime.now() - timedelta(days=730)
        logging.debug('Stock %s: New ticker detected', row['stock_code'])
    elif ( row['last_quote'] + timedelta(days=7) > datetime.now().date() ):
        # we're within the last 7 days so we're already up to date, and there's nothing to do
        logging.debug('Stock %s: Already up to date', row['stock_code'])
        continue
    else:
        # do have quotes for this stock, but we're not up to date
        periodStart = row['last_quote'] + timedelta(days=7)

    try:
        thisTicker = yf.Ticker(row['stock_code'] + '.AX').history(start=periodStart, end=periodEnd, interval='1wk', actions=False)
    except Exception as e:
        logging.error('Stock %s: Unable to pull YF object. Error: ', row['stock_code'], Exception)
        
    # for each record returned by yf (one record = one week)
    for tickerRow in thisTicker.itertuples():
        # Index = yyyy-mm-dd 00:00:00
        # it would probably be better to interpret these as a timestamp and then format to just keep yyyy-mm-dd
        # ...but yolo
        thisDate = str(tickerRow.Index)[:10]
        val.append(
            dict(
                {
                    'quote_date': thisDate,
                    'stock_code': row['stock_code'],
                    'open': cleanNaN(tickerRow.Open),
                    'high': cleanNaN(tickerRow.High),
                    'low': cleanNaN(tickerRow.Low),
                    'close': cleanNaN(tickerRow.Close),
                    'volume': cleanNaN(tickerRow.Volume)
                }
            )
        )
        
    if len(val) > 0:
        # throw it into SQS
        try:
            response = queueClient.send_message(
                QueueUrl = queueUrl,
                DelaySeconds = 0,
                MessageAttributes = {
                    'stock_code': {
                        'DataType': 'String',
                        'StringValue': row['stock_code']
                    }
                },
                MessageBody = (json.dumps(val))
            )
            
            logging.info('Stock %s: Sent %s quotes since %s to SQS as message ID %s', row['stock_code'], str(len(val)), str(val[0]['quote_date']), response['MessageId'])

        except Exception as e:
            logging.error('Stock %s: Failed to send SQS message', str(row['stock_code']), str(Exception))

    else:
        logging.debug('Stock %s: No new quotes to load', row['stock_code'])
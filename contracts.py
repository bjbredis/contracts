from redis import Redis
from redis.exceptions import ResponseError,ConnectionError
from os import environ,urandom,fork
from uuid import uuid1
import random
from time import sleep
from logging import debug, info
from redisearch import Client
from faker import Faker

'''
    REDIS_HOSTNAME optional (default localhost)
    REDIS_PORT optional (default 6379)
    INDEX_NAME 
    COUNT (default 100000)
    

    Example:
    $ export REDIS_HOSTNAME=localhost; export REDIS_PORT=14000; 
    export INDEX_NAME=contracts; export COUNT=1000; 
    python3 contracts.py

'''

'''
returns a coherent record for an options contract



'''
def create_record(fake):
    record = {}
    ticker_symbols = ["TSLA","GOOG","REDIS","GE","AAPL","VISA","JPMC","NVDA","NFLX"]
    expiries = [1576636200, 1577068200, 1606790200,1590978200,1596249200,1564626200]
    ticker = ticker_symbols[random.randrange(0,len(ticker_symbols)-1)]

    record["price"] = round(random.random()*240,2)
    record["product"] = "{}-P{}".format(ticker,random.randrange(1,60))
    record["market"] = "US"
    record["qty"] = random.randrange(1,200000,step=250)
    record["type"] = random.choice(["put","call"])
    record["value"] = record["qty"]*record["price"]
    record["expiry"] = expiries[random.randrange(0,len(expiries)-1)]
    record["delivery_class"] = "{}-DC".format(ticker)
    record["delivery_component"] = random.choice(["{}-equity".format(ticker),"${}USD".format(random.randrange(1,240))])
    record["details"] = "acct_id:{},owner:{},date:{}".format(fake.random_number(digits=9),fake.name(),fake.iso8601(tzinfo=None, end_datetime=None))

    return record




if __name__ == '__main__':

    if 'INDEX_NAME' in environ:
        index_name = environ.get('INDEX_NAME')
    else:
        print("No stream name specified. Please add INDEX_NAME to your environment vars.")
        exit()

    count = environ.get('COUNT',100000)
    redis_hostname = environ.get('REDIS_HOSTNAME','localhost')
    redis_port = environ.get('REDIS_PORT',6379)

    rs = Client(index_name, redis_hostname, redis_port)
    fake = Faker()

    for _ in range(round(int(count))):
        record = create_record(fake)
        uuid = "{}".format(uuid1())
        try: 
            rs.add_document(uuid,replace=True, **record)
            # print(".", end="",flush=True)
        except ResponseError as e:
            print("Dupe with UUID={}".format(uuid))
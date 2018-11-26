import sys
import pandas as pd
from pymongo import MongoClient
assert sys.version_info >= (3, 5)

# retrive the database username and password
dbname = "cdr"
uname = 'test123'
upass = 'test123'
hostt="ds125623.mlab.com"
portt="25623"
mongo_url = ""


def _connect_mongo(host=hostt, port=portt, username=uname, password=upass, db=dbname):
    """ A util for making a connection to mongo """
    if username and password:
        # set the mongoDB URL
        mongo_url = 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, db)
        conn = MongoClient(mongo_url, connectTimeoutMS=30000)
    else:
        conn = MongoClient(host, port)
    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB, using the default
    db = _connect_mongo()

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df

def _load_data(collection, query={}, no_id=True):
    """
    Read from Mongo and return pandas DataFrame
    :return: a pandas dataframe of customers information retrieved from Mlib
    """
    # Connect to MongoDB
    db = _connect_mongo()

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))

    if no_id:
        del df['_id']
    return df


def load_customer(query={}):
    """
    :param: a jason file containing conditions on the customers. for returning all customers no need to pass any parameters
    :return: pandas dataframe of all customers
    """
    customer_df =_load_data(collection="customer_records",query=query,no_id=True)
    return customer_df


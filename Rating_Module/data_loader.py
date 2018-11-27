import sys
import pandas as pd
from pymongo import MongoClient
assert sys.version_info >= (3, 5)


class data_loader:
    __dbname = ""
    __uname = ""
    __upass = ""
    __hostt = ""
    __portt = ""
    __mongo_url = ""

    def __init__(self, host='ds125623.mlab.com', port='25623', username='test123', password='test123', db='cdr'):
        # retrive the database username and password
        self.__dbname = db
        self.__uname = username
        self.__upass = password
        self.__hostt = host
        self.__portt = port
        self.__mongo_url = 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, db)

    def __connect_mongo(self):
        """ A util for making a connection to mongo """
        if "" != self.__mongo_url:
            conn = MongoClient(self.__mongo_url, connectTimeoutMS=30000)
        else:
            conn = MongoClient(self.__host, self.__port)
        return conn[self.__dbname]

    def __load_data(self, collection, query={}, no_id=True):
        """
        Read from Mongo and return pandas DataFrame
        :return: a pandas dataframe of customers information retrieved from Mlib
        """
        # Connect to MongoDB
        db =self.__connect_mongo()

        # Make a query to the specific DB and Collection
        cursor = db[collection].find(query)

        # Expand the cursor and construct the DataFrame
        df = pd.DataFrame(list(cursor))

        if no_id:
            del df['_id']
        return df

    def load_customer(self, query={}):
        """
        :param: a jason file containing conditions on the customers. for returning all customers no need to pass any parameters
        :return: pandas dataframe of all customers
        """

        customer_df =self.__load_data(collection="customer_records", query=query, no_id=True)
        return customer_df

    def load_offers(self, query={}):
        """
        :param: a jason file containing conditions on the offers. for returning all customers no need to pass any parameters
        :return: pandas dataframe of all offers
        """
        offers_df =self.__load_data(collection="customer_records", query=query, no_id=True)
        return offers_df

if __name__ == "__main__":
    dl = data_loader()
    df = dl.load_customer()
    print(df)

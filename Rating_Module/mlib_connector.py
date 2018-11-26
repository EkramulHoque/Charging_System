import pprint,sys
from pymongo import MongoClient
from bson.objectid import ObjectId

assert sys.version_info >= (3, 5)

# retrive the database username and password
dbname = "cdr"
username = 'test123'
password = 'test123'
# set the mongoDB URL
mongo_url = 'mongodb://{0}:{0}@ds125623.mlab.com:25623/cdr'.format(username, password)
client = MongoClient(mongo_url, connectTimeoutMS=30000)
# retrieve the default database
# db = client.get_default_database()

# retrive a spesefic mongo , selecting by name
db = client.get_database("cdr")
customer_records = db.customer_records


def customer_by_id(c_id, select_tag=1):
    """
    return a customers with _id assigned by mongoDB
    cid: the ObjectId generated by MongoDB
    select_tag = 1 to return first record, 0 to to all
    se
    """
    cust = customer_records.find({'_id': ObjectId(c_id)})
    return cust


def customer_by_name(c_name):
    cust = customer_records.find_one({'cname': c_name})
    return cust


def customer_all():
    cust = customer_records
    return cust


def push_record(record):
    return customer_records.insert_one(record).inserted_id


def update_record(record, updates):
    customer_records.update_one({'_id': record['_id']}, {
        '$set': updates
    }, upsert=False)


if __name__ == "__main__":
    jsoon={
        'cname':'Ramin',
        'clast':'Rodaki',
        'cnumber':12345678
    }
    #c_id=push_record(jsoon)
    c2 = customer_by_id('5bfbb91d346c4b3adc3c3106')

    for i in c2:
        pprint.pprint(i)




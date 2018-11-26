import pandas as pd
from Rating_Module import mlib_connector, data_loader
import pprint


def t001_mlibconnector():
    my_obj = {
        "cname": "ramesh",
        "clast": "katebi"
    }
    cid = mlib_connector.push_record(my_obj)
    c = mlib_connector.customer_by_id(cid)
    for i in c:
        pprint.pprint(i)
    print("hi")

def t002_data_loader_customer():
         df= data_loader.load_customer()
         print(df)



if __name__ == "__main__":
    #t001_mlibconnector()
    #t002_data_loader_customer()
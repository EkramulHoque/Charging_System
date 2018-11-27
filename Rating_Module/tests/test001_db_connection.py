import pandas as pd
from Rating_Module import mlib_connector, data_loader
import pprint

class Unit_test:
    def t001_mlibconnector(self):
        my_obj = {
            "cname": "ramesh",
            "clast": "katebi"
        }
        cid = mlib_connector.push_record(my_obj)
        c = mlib_connector.customer_by_id(cid)
        for i in c:
            pprint.pprint(i)
        print("hi")

    def t002_data_loader_customer(self):
             df= data_loader.load_customer()
             print(df)



if __name__ == "__main__":
    ut = Unit_test()
    #ut.t001_mlibconnector()
    ut.t002_data_loader_customer()

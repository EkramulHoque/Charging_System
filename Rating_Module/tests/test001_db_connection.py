from Rating_Module.data_loader import data_loader
from Rating_Module import mlib_connector
import pprint


class UnitTest:

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

    @staticmethod
    def t002_data_loader_customer():
        dl = data_loader()
        df = dl.load_customer()
        print(df)


if __name__ == "__main__":
    ut = UnitTest()
    #ut.t001_mlibconnector()
    ut.t002_data_loader_customer()
